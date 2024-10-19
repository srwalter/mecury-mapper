use std::collections::HashMap;
use std::io::{prelude::*, BufReader};
use std::io::{self, ErrorKind, SeekFrom};
use std::fs::{File, OpenOptions};
use std::ops::Sub;

use serde::{Deserialize, Serialize};
use devicemapper::{DM, Device, DevId, DmName, DmOptions, DmError, Sectors, TargetTable};
use nix::sys::stat;

#[derive(Serialize,Deserialize,Debug)]
pub struct SuperPartition {
    device: String,
    generation: u32,
    pub subvols: HashMap<String, SubVolume>
}

// Can describe metadata for GPT partitions by creating a subvolume with
// the same name and no extents
//
// Encode a "metadata" partition for the last two blocks so we don't
#[derive(Serialize,Deserialize,PartialEq,Debug,Clone)]
// have to special-case them for block allocation
pub struct SubVolume {
    extents: Vec<Extent>,
    version: String,
    author: String,
    timedate: String,
}

#[derive(Serialize,Deserialize,PartialEq,Debug,Eq,PartialOrd,Ord,Clone)]
struct Extent {
    block_offset: u64,
    block_length: u64,
}

// XXX: this needs to be something reliably derived from an intrinsic
// property of the hardware, not something that can change over time
fn get_io_size(device: &str) -> Result<u64, io::Error> {
    // FIXME
    Ok(1024 * 1024)
}

fn load_metadata(f: &mut File) -> Result<SuperPartition, io::Error> {
    let mut disk_crc = [0; 4];
    f.read_exact(&mut disk_crc)?;
    let disk_crc = u32::from_be_bytes(disk_crc);

    let mut buf = BufReader::new(f);
    let mut json_metadata = "".to_string();
    buf.read_line(&mut json_metadata)?;
    let crc_algo = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
    let actual_crc = crc_algo.checksum(json_metadata.trim().as_bytes());
    if disk_crc == actual_crc {
        let metadata = serde_json::from_str::<SuperPartition>(&json_metadata);
        metadata.map_err(|_x| io::Error::new(ErrorKind::InvalidData, "can't parse json"))
    } else {
        Err(io::Error::new(ErrorKind::InvalidData, "crc doesn't match"))
    }
}

fn load_both_metadata(mut blockdev: &mut File, iosize: u64) -> Result<(Option<SuperPartition>, Option<SuperPartition>), io::Error> {
    let device_size = blockdev.seek(SeekFrom::End(0))?;
    let device_size_blocks = device_size / iosize;

    blockdev.seek(SeekFrom::Start((device_size_blocks-1) * iosize))?;
    let meta1 = load_metadata(&mut blockdev).ok();
    blockdev.seek(SeekFrom::Start((device_size_blocks-2) * iosize))?;
    let meta2 = load_metadata(&mut blockdev).ok();

    Ok((meta1, meta2))
}

impl SuperPartition {
    /// Open an existing super partition with on-disk metadata
    pub fn open(device: String) -> Result<Self, io::Error> {
        let mut blockdev = File::open(&device)?;
        let iosize = get_io_size(&device)?;
        let (meta1, meta2) = load_both_metadata(&mut blockdev, iosize)?;

        let mut meta = match (meta1,meta2) {
            (Some(meta), None) => meta,
            (None, Some(meta)) => meta,
            (None, None) => return Err(io::Error::new(ErrorKind::NotFound, "no valid metadata")),
            (Some(meta1), Some(meta2)) => {
                if meta1.generation > meta2.generation {
                    meta1
                } else {
                    meta2
                }
            }
        };
        meta.device = device;

        for (name, sv) in &meta.subvols {
            meta.create_dm(name, sv, iosize).map_err(|e| {
                eprintln!("create_dm {:?}", e);
                io::Error::new(ErrorKind::Other, "create dm")
            })?;
        }
        Ok(meta)
    }

    /// Convert an existing partition into a new super partition.  There
    /// must be enough difference between the partition size and
    /// original_size to allow for 2 blocks for metadata storage.
    pub fn adopt(device: String, name: String, original_size: u64) -> Result<Self, io::Error> {
        let mut blockdev = File::open(&device)?;

        let device_size = blockdev.seek(SeekFrom::End(0))?;
        let iosize = get_io_size(&device)?;
        let device_size_blocks = device_size / iosize;
        let original_size_blocks = (original_size + iosize - 1) / iosize;

        if original_size_blocks + 2 > device_size_blocks {
            return Err(io::Error::new(ErrorKind::OutOfMemory, "not enough room for metadata"));
        }

        let extent = Extent {
            block_offset: device_size_blocks - 2,
            block_length: 2,
        };
        let subvol = SubVolume {
            extents: vec![extent],
            version: "".to_string(),
            author: "".to_string(),
            timedate: "".to_string(),
        };

        let mut subvols = HashMap::new();
        subvols.insert("metadata".to_string(), subvol);

        let extent = Extent {
            block_offset: 0,
            block_length: original_size_blocks,
        };
        let subvol = SubVolume {
            extents: vec![extent],
            version: "".to_string(),
            author: "".to_string(),
            timedate: "".to_string(),
        };
        subvols.insert(name, subvol);

        Ok(Self {
            device,
            generation: 1,
            subvols
        })
    }

    fn get_all_extents(&self) -> Vec<&Extent> {
        let mut extents = vec![];

        for (_k,v) in &self.subvols {
            extents.extend(&v.extents);
        }
        extents.sort();

        extents
    }

    pub fn create_subvol(&mut self, name: String, size: u64) -> Result<(), io::Error> {
        if self.subvols.contains_key(&name) {
            return Err(io::Error::new(ErrorKind::AlreadyExists, "subvol already exists"));
        }
        let iosize = get_io_size(&self.device)?;
        let mut size_blocks = (size + iosize - 1) / iosize;

        let all_extents = self.get_all_extents();
        let mut my_extents = vec![];

        for (a, b) in std::iter::zip(&all_extents, &all_extents[1..]) {
            let hole_start = a.block_offset + a.block_length;
            let hole_len = b.block_offset - hole_start;

            let extent = Extent {
                block_offset: hole_start,
                block_length: std::cmp::min(hole_len, size_blocks),
            };

            size_blocks -= extent.block_length;
            my_extents.push(extent);

            if size_blocks == 0 {
                break;
            }
        }

        if size_blocks > 0 {
            return Err(io::Error::new(ErrorKind::OutOfMemory, "not enough space for subvol"));
        }

        let sv = SubVolume {
            extents: my_extents,
            version: "".to_string(),
            author: "".to_string(),
            timedate: "".to_string(),
        };
        self.subvols.insert(name.clone(), sv.clone());
        self.commit()?;
        self.create_dm(&name, &sv, iosize).map_err(|e| {
            eprintln!("create_dm {:?}", e);
            io::Error::new(ErrorKind::Other, "create dm")
        })?;
        Ok(())
    }

    fn get_major_minor(&self) -> Result<(u32, u32), io::Error> {
        let st = stat::stat(std::path::Path::new(&self.device))?;
        let major = stat::major(st.st_rdev);
        let minor = stat::minor(st.st_rdev);
        Ok((major as u32, minor as u32))
    }

    fn create_dm(&self, name: &str, sv: &SubVolume, iosize: u64) -> Result<(), DmError> {
        let name = DmName::new(name)?;
        let options = DmOptions::default();
        let dm = DM::new()?;

        let mut table = vec![];
        let mut start = 0;
        for e in &sv.extents {
            if e.block_length == 0 {
                continue;
            }

            let (major, minor) = self.get_major_minor().expect("major minor");
            let source_dev = Device {
                major,
                minor,
            };
            let params = devicemapper::LinearTargetParams::new(source_dev, Sectors(e.block_offset * iosize / 512));

            let start_sectors = Sectors(start * iosize / 512);
            let length_sectors = Sectors(e.block_length * iosize / 512);
            let line = devicemapper::TargetLine::new(start_sectors, length_sectors,
                devicemapper::LinearDevTargetParams::Linear(params));
            table.push(line);

            start += e.block_length;
        }

        let id = DevId::Name(name);
        let target = devicemapper::LinearDevTargetTable::new(table);
        dm.device_create(name, None, options)?;
        dm.table_load(&id, &target.to_raw_table(), options)?;
        // Un-suspend the device
        dm.device_suspend(&id, DmOptions::default())?;

        Ok(())
    }

    pub fn delete_subvol(&mut self, sv: SubVolume) -> Result<(), io::Error> {
        self.remove_dm(&sv);
        self.commit()?;
        self.subvols.retain(|_k, v| *v != sv);
        Ok(())
    }

    fn remove_dm(&self, sv: &SubVolume) {
    }

    /// Commit metadata back to storage
    pub fn commit(&mut self) -> Result<(), io::Error> {
        let mut blockdev = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.device)?;
        let device_size = blockdev.seek(SeekFrom::End(0))?;
        let iosize = get_io_size(&self.device)?;
        let device_size_blocks = device_size / iosize;

        let (meta1, meta2) = load_both_metadata(&mut blockdev, iosize)?;

        // Decide which slot to write the new metadata to
        let md_block = match (meta1, meta2) {
            (Some(_meta), None) => 2,
            (None, Some(_meta)) => 1,
            (None, None) => 1,
            (Some(meta1), Some(meta2)) => {
                if meta1.generation < meta2.generation {
                    1
                } else {
                    2
                }
            }
        };

        self.generation += 1;

        let json = serde_json::to_string(&self).expect("json to_string");
        // 4 byte CRC plus newline plus NUL
        assert!(json.len() + 6 < iosize as usize);
        let crc_algo = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
        let actual_crc = crc_algo.checksum(json.as_bytes());
        let crc_bytes = actual_crc.to_be_bytes();

        blockdev.seek(SeekFrom::Start((device_size_blocks-md_block) * iosize))?;
        blockdev.write_all(&crc_bytes)?;
        blockdev.write_all(json.as_bytes())?;
        blockdev.write_all("\n\0".as_bytes())?;
        blockdev.sync_all()?;

        Ok(())
    }
}