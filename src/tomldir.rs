use std::{fs::{self, ReadDir}, io, path::{Path, PathBuf}};

use serde::Deserialize;

#[derive(thiserror::Error, Debug)]
pub enum TomlDirError {
    /// An error occurred during file system operations (reading directory, reading file)
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// An error occurred while deserializing the TOML content
    #[error("TOML parsing error in file: {0}")]
    TomlParse(#[from] toml::de::Error),

    /// The path provided was not a valid directory
    #[error("Invalid path: {0}")]
    PathError(String),
    
    #[error("Could not determine file stem for path: {0}")]
    StemError(PathBuf),
}
// An iterator that lazily reads and parses TOML files from a directory.
pub struct TomlDirIterator<T>
where
    T: for<'de> Deserialize<'de>,
{
    // The inner iterator over directory entries
    dir_entries: ReadDir,
    // Phantom data to link the struct to the type T without holding an instance of T
    _marker: std::marker::PhantomData<T>,
}

impl<T> TomlDirIterator<T>
where
    T: for<'de> Deserialize<'de>,
{
    // Reads and parses a single file.
    fn parse_file(&self, path: &Path) -> Result<T, TomlDirError> {
        let contents = fs::read_to_string(path)?; 

        let config: T = toml::from_str(&contents)?; 
        
        Ok(config)
    }
}

impl<T> Iterator for TomlDirIterator<T>
where
    T: for<'de> Deserialize<'de>,
{
    // The item is a Result, allowing the user to handle parsing errors file-by-file
    type Item = Result<(String, T), TomlDirError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Loop until a valid TOML file is found and parsed, or the directory ends
        loop {
            // Get the next directory entry (returns Option<Result<DirEntry, io::Error>>)
            let entry_result = self.dir_entries.next()?; // Returns None if iteration is complete

            match entry_result {
                Ok(entry) => {
                    let path = entry.path();
                    
                    // Check if the path is a file and ends with ".toml"
                    if path.is_file() && path.extension().is_some_and(|ext| ext == "toml") {
                        // Extract the file stem before processing
                        let file_stem = match path.file_stem().and_then(|s| s.to_str()) {
                            Some(stem) => stem.to_string(),
                            None => return Some(Err(TomlDirError::StemError(path))),
                        };
                        
                        // Attempt to read and parse the file
                        match self.parse_file(&path) {
                            Ok(data) => return Some(Ok((file_stem, data))), // Success! Return the parsed config
                            Err(e) => return Some(Err(e)),         // Parsing or IO error on this file
                        }
                    }
                }
                Err(e) => {
                    // IO error reading the directory itself (e.g., permissions)
                    return Some(Err(TomlDirError::from(e))); 
                }
            }
        }
    }
}

/// Returns an iterator over the parsed configurations in a directory.
pub fn parse<T>(dir_path: &Path) -> Result<TomlDirIterator<T>, TomlDirError>
where
    T: for<'de> Deserialize<'de>,
{
    if !dir_path.is_dir() {
        return Err(TomlDirError::PathError(format!(
            "Path '{}' is not a directory.",
            dir_path.display()
        )));
    }

    let dir_entries = fs::read_dir(dir_path)?; 

    Ok(TomlDirIterator {
        dir_entries,
        _marker: std::marker::PhantomData,
    })
}