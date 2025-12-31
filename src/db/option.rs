use iroh::EndpointId;
use redb::{
    Error, ReadOnlyTable, ReadTransaction, ReadableTable, Table, TableDefinition, WriteTransaction,
};
use thiserror::Error;

pub const TABLE: TableDefinition<u8, &[u8]> = TableDefinition::new("option");

#[derive(Debug)]
enum DbOption {
    Engine = 0,
    SecretKey = 1,
    Remotes = 2,
}

#[derive(Error, Debug)]
pub enum OptionError {
    #[error("error serde")]
    PostcardError(#[from] postcard::Error),

    #[error("could not read from table")]
    StorageError(#[from] redb::StorageError),

    #[error("could not find option")]
    OptionNotSet,

    #[error("Remote not found: {0}")]
    RemoteNotFound(String),
}

pub struct OptionTable<T> {
    table: T,
}

// Getters (Available for both Read and Write tables)
impl<T: ReadableTable<u8, &'static [u8]>> OptionTable<T> {
    pub fn get_engine(&self) -> Result<crate::db::Engine, OptionError> {
        let v = self
            .table
            .get(DbOption::Engine as u8)?
            .ok_or(OptionError::OptionNotSet)?;

        Ok(postcard::from_bytes(v.value())?)
    }

    pub fn get_secret_key(&self) -> Result<iroh::SecretKey, OptionError> {
        let v = self
            .table
            .get(DbOption::SecretKey as u8)?
            .ok_or(OptionError::OptionNotSet)?;

        let bytes: [u8; 32] = v
            .value()
            .try_into()
            .map_err(|_| OptionError::OptionNotSet)?;
        Ok(iroh::SecretKey::from_bytes(&bytes))
    }

    pub fn get_all_remotes(&self) -> Result<Vec<(String, EndpointId)>, OptionError> {
        let v = self
            .table
            .get(DbOption::Remotes as u8)?
            .ok_or(OptionError::OptionNotSet)?;

        Ok(postcard::from_bytes(v.value())?)
    }

    pub fn get_remote(&self, name: &str) -> Result<EndpointId, OptionError> {
        let remotes = self.get_all_remotes()?;
        let (_, bytes) = remotes
            .into_iter()
            .find(|(n, _)| n == name)
            .ok_or_else(|| OptionError::RemoteNotFound(name.to_string()))?;

        Ok(bytes)
    }
}

// Setters (Available ONLY for Write tables)
impl<'txn> OptionTable<Table<'txn, u8, &'static [u8]>> {
    pub fn set_engine(&mut self, engine: crate::db::Engine) -> Result<(), OptionError> {
        let bytes = postcard::to_stdvec(&engine)?;
        self.table
            .insert(DbOption::Engine as u8, bytes.as_slice())?;
        Ok(())
    }

    pub fn reset_secret_key(&mut self) -> Result<(), OptionError> {
        let secret = iroh::SecretKey::generate(&mut rand::rng());
        self.table
            .insert(DbOption::SecretKey as u8, secret.to_bytes().as_slice())?;
        Ok(())
    }

    fn set_remotes(&mut self, remotes: Vec<(String, EndpointId)>) -> Result<(), OptionError> {
        let bytes = postcard::to_stdvec(&remotes)?;
        self.table
            .insert(DbOption::Remotes as u8, bytes.as_slice())?;
        Ok(())
    }

    pub fn add_remote(&mut self, name: &str, endpoint_id: &EndpointId) -> Result<(), OptionError> {
        let mut remotes = match self.get_all_remotes() {
            Ok(r) => r,
            Err(OptionError::OptionNotSet) => Vec::new(),
            Err(e) => return Err(e.into()),
        };

        remotes.retain(|(n, _)| n != name);
        remotes.push((name.to_string(), *endpoint_id));

        self.set_remotes(remotes)?;
        Ok(())
    }

    pub fn remove_remote(&mut self, name: &str) -> Result<(), OptionError> {
        let mut remotes = match self.get_all_remotes() {
            Ok(r) => r,
            Err(OptionError::OptionNotSet) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        remotes.retain(|(n, _)| n != name);
        self.set_remotes(remotes)?;
        Ok(())
    }
}

pub trait OptionExt {
    type TableType<'a>
    where
        Self: 'a;
    fn option_table(&self) -> Result<OptionTable<Self::TableType<'_>>, Error>;
}

// Implementation for Read Transactions
impl OptionExt for ReadTransaction {
    type TableType<'a> = ReadOnlyTable<u8, &'static [u8]>;

    fn option_table(&self) -> Result<OptionTable<Self::TableType<'_>>, Error> {
        let table = self.open_table(TABLE)?;
        Ok(OptionTable { table })
    }
}

// Implementation for Write Transactions
impl OptionExt for WriteTransaction {
    type TableType<'a> = Table<'a, u8, &'static [u8]>;

    fn option_table(&self) -> Result<OptionTable<Self::TableType<'_>>, redb::Error> {
        let table = self.open_table(TABLE)?;
        Ok(OptionTable { table })
    }
}
