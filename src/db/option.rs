use redb::{Error, ReadOnlyTable, ReadTransaction, ReadableTable, Table, TableDefinition, WriteTransaction};
use thiserror::Error;

pub const TABLE: TableDefinition<u8, &[u8]> = TableDefinition::new("option");

#[derive(Debug)]
enum DbOption {
    Engine = 0,
    SecretKey = 1,
}

#[derive(Error, Debug)]
pub enum OptionError {
    #[error("error serde")]
    PostcardError(#[from] postcard::Error),

    #[error("could not read from table")]
    StorageError(#[from] redb::StorageError),

    #[error("could not find option")]
    OptionNotSet,
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

        let bytes: [u8; 32] = v.value().try_into().map_err(|_| OptionError::OptionNotSet)?;
        Ok(iroh::SecretKey::from_bytes(&bytes))
    }
}

// Setters (Available ONLY for Write tables)
impl<'txn> OptionTable<Table<'txn, u8, &'static [u8]>> {
    pub fn set_engine(&mut self, val: crate::db::Engine) -> Result<(), OptionError> {
        let bytes = postcard::to_stdvec(&val)?;
        self.table.insert(DbOption::Engine as u8, bytes.as_slice())?;
        Ok(())
    }

    pub fn reset_secret_key(&mut self) -> Result<(), OptionError> {
        let secret = iroh::SecretKey::generate(&mut rand::rng());
        self.table.insert(DbOption::SecretKey as u8, secret.to_bytes().as_slice())?;
        Ok(())
    }
}

pub trait OptionExt {
    type TableType<'a> where Self: 'a;
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