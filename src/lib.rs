mod errors;

use std::{fs, result};
use std::error::Error;
use std::io;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::MetadataExt;
use std::path;
use std::path::PathBuf;
use std::sync;
use std::time;
use crossbeam::channel::Receiver;
use errors::Error as InnerError;

pub type Bytes = Vec<u8>;
pub type Result = result::Result<(), InnerError>;

pub trait Queue {
    fn put(&self, bytes: Bytes) -> Result;
    fn read_chan(&self) -> Receiver<Bytes>;
    fn peek_chan(&self) -> Receiver<Bytes>;
    fn close(self) -> Result;
    fn delete(&mut self) -> Result;
    fn depth(&self) -> i64;
    fn empty(&mut self) -> Result;
    fn run(&self) -> Result;
}

#[derive(Default)]
struct DiskQueueState {
    read_pos: i64,
    write_pos: i64,
    read_file_number: i64,
    write_file_number: i64,
    depth: i64,
}

pub struct DiskQueue {
    state: sync::RwLock<DiskQueueState>,

    name: String,
    data_path: path::PathBuf,
    max_bytes_per_file: i64,
    max_bytes_per_file_read: i64,
    min_msg_size: i32,
    max_msg_size: i32,
    sync_every: i64,
    sync_timeout: time::Duration,
    exit_flag: i32,
    need_sync: bool,

    next_read_pos: i64,
    next_read_file_number: i64,

    read_file: Option<fs::File>,
    write_file: Option<fs::File>,
    reader: Option<BufReader<Bytes>>,
    write_buf: Option<BufReader<Bytes>>,
}

impl DiskQueue {
    pub fn new(name: String, path: String, max_bytes_per_file: i64, min_msg_size: i32, max_msg_size: i32, sync_timeout: time::Duration) -> Self {
        let dq = DiskQueue {
            state: Default::default(),
            name,
            data_path: path.into(),
            max_bytes_per_file,
            max_bytes_per_file_read: 0,
            min_msg_size,
            max_msg_size,
            sync_every: 0,
            sync_timeout,
            exit_flag: 0,
            need_sync: false,
            next_read_pos: 0,
            next_read_file_number: 0,
            read_file: None,
            write_file: None,
            reader: None,
            write_buf: None,
        };

        dq
    }

    fn restrict_metadata(&mut self) -> Result {
        let metadata_path = self.metadata_filename();
        let metadata_file = fs::OpenOptions::new().read(true).open(metadata_path).map_err(|e| {
            InnerError::OpenMetadataFailed(e.to_string())
        })?;
        // metadata大概如下
        // depth
        // read_file_number,read_pos
        // write_file_number,write_pos
        let lines: Vec<String> = BufReader::new(metadata_file).lines().filter(|x| {
            x.is_ok()
        }).map(|x| {
            x.unwrap()
        }).collect();

        if lines.len() != 3 {
            return Err(InnerError::InvalidMetadataFile);
        }

        // read depth
        let depth: i64 = lines[0].parse().map_err(|e| {
            InnerError::InvalidMetadataFile
        })?;

        fn parse_line(s: &str) -> result::Result<(i64, i64), InnerError> {
            let line = s;
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() != 2 {
                return Err(InnerError::InvalidMetadataFile);
            }

            let part_1 = parts[0].parse().map_err(|e| {
                InnerError::InvalidMetadataFile
            })?;
            let part_2 = parts[1].parse().map_err(|e| {
                InnerError::InvalidMetadataFile
            })?;

            Ok((part_1, part_2))
        }

        // read read_file_number,read_pos
        let (read_file_number, read_pos) = parse_line(&lines[1])?;
        // read write_file_number,write_pos
        let (write_file_number, write_pos) = parse_line(&lines[2])?;

        {
            let mut state = self.state.write().unwrap();
            state.depth = depth;
            state.read_file_number = read_file_number;
            state.read_pos = read_pos;
            state.write_file_number = write_file_number;
            state.write_pos = read_pos;
        }

        self.next_read_file_number = read_file_number;
        self.next_read_pos = read_pos;

        let filename = self.filename(read_pos);
        let stat = fs::metadata(filename).map_err(|e| {
            InnerError::OpenMetadataFailed(e.to_string())
        })?;

        let file_size = stat.size();
        if (write_pos as u64) < file_size {
            {
                let mut state = self.state.write().unwrap();
                state.write_file_number += 1;
                state.write_pos = 0;
            }

            self.write_file.take();
        }

        Ok(())
    }

    fn metadata_filename(&self) -> PathBuf {
        self.data_path.join(&format!("%{}.diskqueue.meta.dat", self.name))
    }

    fn filename(&self, file_num: i64) -> PathBuf {
        self.data_path.join(&format!("%{}.diskqueue.{:06}.dat", self.name, file_num))
    }
}