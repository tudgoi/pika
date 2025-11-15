use std::{fs::{self, ReadDir}, io, path::{Path, PathBuf}};

#[derive(thiserror::Error, Debug)]
pub enum ParseDirError<E> {
    /// An error occurred during file system operations (reading directory, reading file)
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// An error occurred while deserializing the content
    #[error("Parsing error in file: {0}")]
    FileParse(E),

    /// The path provided was not a valid directory
    #[error("Invalid path: {0}")]
    PathError(String),
    
    #[error("Could not determine file stem for path: {0}")]
    StemError(PathBuf),
}

// An iterator that lazily reads and parses files from a directory using a provided parser function.
pub struct ParseDirIterator<T, F>
{
    // The inner iterator over directory entries
    dir_entries: ReadDir,
    // The function to parse a file
    parser: F,
    // Phantom data to link the struct to the type T without holding an instance of T
    _marker: std::marker::PhantomData<T>,
}

impl<T, F, E> Iterator for ParseDirIterator<T, F>
where
    F: Fn(&str) -> Result<T, E>,
{
    // The item is a Result, allowing the user to handle parsing errors file-by-file
    type Item = Result<(String, T), ParseDirError<E>>;

    fn next(&mut self) -> Option<Self::Item> {
        // Loop until a valid file is found and parsed, or the directory ends
        loop {
            // Get the next directory entry (returns Option<Result<DirEntry, io::Error>>)
            let entry_result = self.dir_entries.next()?; // Returns None if iteration is complete

            match entry_result {
                Ok(entry) => {
                    let path = entry.path();
                    
                    // Check if the path is a file
                    if path.is_file() {
                        // Extract the file stem before processing
                        let file_stem = match path.file_stem().and_then(|s| s.to_str()) {
                            Some(stem) => stem.to_string(),
                            None => return Some(Err(ParseDirError::StemError(path))),
                        };
                        
                        // Attempt to read and parse the file using the provided parser
                        let contents = match fs::read_to_string(path) {
                            Ok(contents) => contents,
                            Err(e) => return Some(Err(ParseDirError::Io(e))),
                        };
                        match (self.parser)(&contents) {
                            Ok(data) => return Some(Ok((file_stem, data))), // Success! Return the parsed data
                            Err(e) => return Some(Err(ParseDirError::FileParse(e))), // Parsing error on this file
                        }
                    }
                }
                Err(e) => {
                    // IO error reading the directory itself (e.g., permissions)
                    return Some(Err(ParseDirError::from(e))); 
                }
            }
        }
    }
}

/// Returns an iterator over the parsed configurations in a directory.
pub fn parse<T, F, E>(dir_path: &Path, parser: F) -> Result<ParseDirIterator<T, F>, ParseDirError<E>>
where
    F: Fn(&str) -> Result<T, E>,
{
    if !dir_path.is_dir() {
        return Err(ParseDirError::PathError(format!(
            "Path '{}' is not a directory.",
            dir_path.display()
        )));
    }

    let dir_entries = fs::read_dir(dir_path)?; 

    Ok(ParseDirIterator {
        dir_entries,
        parser,
        _marker: std::marker::PhantomData,
    })
}