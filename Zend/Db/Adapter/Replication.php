<?php
/**
 * Replication.php
 */
class Zend_Db_Adapter_Replication extends Zend_Db_Adapter_Abstract
{
    protected $_in_transaction = false;

    /* @var Zend_Db_Adapter_Abstract $_master */
    protected $_master;
    /* @var Zend_Db_Adapter_Abstract $_slave */
    protected $_slave;

    /**
     * Overridden Methods
     */

    /**
     * Constructor.
     *
     *
     */
    public function __construct($master, $slave)
    {
        $this->_master = $master;
        $this->_slave = $slave;
    }

    /**
     * Returns the underlying database connection object or resource.
     * If not presently connected, this initiates the connection.
     *
     * @return object|resource|null
     */
    public function getConnection()
    {
        $this->guardSlave(function($slave)
        {
            $slave->getConnection();
        });

        return $this->_master->getConnection();
    }

    /**
     * Returns the configuration variables in this adapter.
     *
     * @return array
     */
    public function getConfig()
    {
        return $this->_master->getConfig();
    }

    /**
     * Set the adapter's profiler object.
     *
     * The argument may be a boolean, an associative array, an instance of
     * Zend_Db_Profiler, or an instance of Zend_Config.
     *
     * A boolean argument sets the profiler to enabled if true, or disabled if
     * false.  The profiler class is the adapter's default profiler class,
     * Zend_Db_Profiler.
     *
     * An instance of Zend_Db_Profiler sets the adapter's instance to that
     * object.  The profiler is enabled and disabled separately.
     *
     * An associative array argument may contain any of the keys 'enabled',
     * 'class', and 'instance'. The 'enabled' and 'instance' keys correspond to the
     * boolean and object types documented above. The 'class' key is used to name a
     * class to use for a custom profiler. The class must be Zend_Db_Profiler or a
     * subclass. The class is instantiated with no constructor arguments. The 'class'
     * option is ignored when the 'instance' option is supplied.
     *
     * An object of type Zend_Config may contain the properties 'enabled', 'class', and
     * 'instance', just as if an associative array had been passed instead.
     *
     * @param  Zend_Db_Profiler|Zend_Config|array|boolean $profiler
     * @return Zend_Db_Adapter_Abstract Provides a fluent interface
     * @throws Zend_Db_Profiler_Exception if the object instance or class specified
     *         is not Zend_Db_Profiler or an extension of that class.
     */
    public function setProfiler($profiler)
    {
        $this->guardSlave(function($slave) use ($profiler)
        {
            $slave->setProfiler($profiler);
        });


        return $this->_master->setProfiler($profiler);
    }


    /**
     * Returns the profiler for this adapter.
     *
     * @return Zend_Db_Profiler
     */
    public function getProfiler()
    {
        return $this->_master->getProfiler();
    }

    /**
     * Get the default statement class.
     *
     * @return string
     */
    public function getStatementClass()
    {
        return $this->_master->_defaultStmtClass;
    }

    /**
     * Set the default statement class.
     *
     * @return Zend_Db_Adapter_Abstract Fluent interface
     */
    public function setStatementClass($class)
    {
        $this->guardSlave(function($slave) use ($class)
        {
           $slave->_defaultStmtClass = $class;
        });

        $this->_defaultStmtClass = $class;
        return $this;
    }

    /**
     * Prepares and executes an SQL statement with bound data.
     *
     * @param  mixed  $sql  The SQL statement with placeholders.
     *                      May be a string or Zend_Db_Select.
     * @param  mixed  $bind An array of data to bind to the placeholders.
     * @return Zend_Db_Statement_Interface
     */
    public function query($sql, $bind = array())
    {
        $sql_string = strtolower(trim($sql));
        if(strpos($sql_string, 'session') > 0)
        {
            $sql_string .= 'blah';
        }
        if(strpos($sql_string,'select') === 0 && $this->_slave != null && !$this->_in_transaction)
        {
            return $this->_slave->query($sql, $bind);
        }

        return $this->_master->query($sql, $bind);
    }

    /**
     * Leave autocommit mode and begin a transaction.
     *
     * @return Zend_Db_Adapter_Abstract
     */
    public function beginTransaction()
    {
        $this->_in_transaction = true;
        return $this->_master->beginTransaction();
    }

    /**
     * Commit a transaction and return to autocommit mode.
     *
     * @return Zend_Db_Adapter_Abstract
     */
    public function commit()
    {
        $this->_in_transaction = false;
        return $this->_master->commit();
    }

    /**
     * Roll back a transaction and return to autocommit mode.
     *
     * @return Zend_Db_Adapter_Abstract
     */
    public function rollBack()
    {
        $this->_in_transaction = false;
        return $this->_master->rollBack();
    }

    /**
     * Inserts a table row with specified data.
     *
     * @param mixed $table The table to insert data into.
     * @param array $bind Column-value pairs.
     * @return int The number of affected rows.
     * @throws Zend_Db_Adapter_Exception
     */
    public function insert($table, array $bind)
    {
        return $this->_master->insert($table, $bind);
    }

    /**
     * Updates table rows with specified data based on a WHERE clause.
     *
     * @param  mixed        $table The table to update.
     * @param  array        $bind  Column-value pairs.
     * @param  mixed        $where UPDATE WHERE clause(s).
     * @return int          The number of affected rows.
     * @throws Zend_Db_Adapter_Exception
     */
    public function update($table, array $bind, $where = '')
    {
       $this->_master->update($table, $bind, $where);
    }

    /**
     * Deletes table rows based on a WHERE clause.
     *
     * @param  mixed        $table The table to update.
     * @param  mixed        $where DELETE WHERE clause(s).
     * @return int          The number of affected rows.
     */
    public function delete($table, $where = '')
    {
        return $this->_master->delete($table, $where);
    }

    /**
     * Creates and returns a new Zend_Db_Select object for this adapter.
     *
     * @return Zend_Db_Select
     */
    public function select()
    {
        if($this->_slave != null && !$this->_in_transaction)
        {
            return $this->_slave->select();
        }

        return $this->_master->select();
    }

    /**
     * Get the fetch mode.
     *
     * @return int
     */
    public function getFetchMode()
    {
        return $this->_master->getFetchMode();
    }

    /**
     * Fetches all SQL result rows as a sequential array.
     * Uses the current fetchMode for the adapter.
     *
     * @param string|Zend_Db_Select $sql  An SQL SELECT statement.
     * @param mixed                 $bind Data to bind into SELECT placeholders.
     * @param mixed                 $fetchMode Override current fetch mode.
     * @return array
     */
    public function fetchAll($sql, $bind = array(), $fetchMode = null)
    {
        if ($fetchMode === null) {
            $fetchMode = $this->getFetchMode();
        }
        $stmt = $this->query($sql, $bind);
        $result = $stmt->fetchAll($fetchMode);
        return $result;
    }

    /**
     * Fetches the first row of the SQL result.
     * Uses the current fetchMode for the adapter.
     *
     * @param string|Zend_Db_Select $sql An SQL SELECT statement.
     * @param mixed $bind Data to bind into SELECT placeholders.
     * @param mixed                 $fetchMode Override current fetch mode.
     * @return array
     */
    public function fetchRow($sql, $bind = array(), $fetchMode = null)
    {
        if ($fetchMode === null) {
            $fetchMode = $this->getFetchMode();
        }
        $stmt = $this->query($sql, $bind);
        $result = $stmt->fetch($fetchMode);
        return $result;
    }

    /**
     * Fetches all SQL result rows as an associative array.
     *
     * The first column is the key, the entire row array is the
     * value.  You should construct the query to be sure that
     * the first column contains unique values, or else
     * rows with duplicate values in the first column will
     * overwrite previous data.
     *
     * @param string|Zend_Db_Select $sql An SQL SELECT statement.
     * @param mixed $bind Data to bind into SELECT placeholders.
     * @return array
     */
    public function fetchAssoc($sql, $bind = array())
    {
        $stmt = $this->query($sql, $bind);
        $data = array();
        while ($row = $stmt->fetch(Zend_Db::FETCH_ASSOC)) {
            $tmp = array_values(array_slice($row, 0, 1));
            $data[$tmp[0]] = $row;
        }
        return $data;
    }

    /**
     * Fetches the first column of all SQL result rows as an array.
     *
     * @param string|Zend_Db_Select $sql An SQL SELECT statement.
     * @param mixed $bind Data to bind into SELECT placeholders.
     * @return array
     */
    public function fetchCol($sql, $bind = array())
    {
        $stmt = $this->query($sql, $bind);
        $result = $stmt->fetchAll(Zend_Db::FETCH_COLUMN, 0);
        return $result;
    }

    /**
     * Fetches all SQL result rows as an array of key-value pairs.
     *
     * The first column is the key, the second column is the
     * value.
     *
     * @param string|Zend_Db_Select $sql An SQL SELECT statement.
     * @param mixed $bind Data to bind into SELECT placeholders.
     * @return array
     */
    public function fetchPairs($sql, $bind = array())
    {
        $stmt = $this->query($sql, $bind);
        $data = array();
        while ($row = $stmt->fetch(Zend_Db::FETCH_NUM)) {
            $data[$row[0]] = $row[1];
        }
        return $data;
    }

    /**
     * Fetches the first column of the first row of the SQL result.
     *
     * @param string|Zend_Db_Select $sql An SQL SELECT statement.
     * @param mixed $bind Data to bind into SELECT placeholders.
     * @return string
     */
    public function fetchOne($sql, $bind = array())
    {
        $stmt = $this->query($sql, $bind);
        $result = $stmt->fetchColumn(0);
        return $result;
    }

    /**
     * Return the most recent value from the specified sequence in the database.
     * This is supported only on RDBMS brands that support sequences
     * (e.g. Oracle, PostgreSQL, DB2).  Other RDBMS brands return null.
     *
     * @param string $sequenceName
     * @return string
     */
    public function lastSequenceId($sequenceName)
    {
        return $this->_master->lastSequenceId($sequenceName);
    }

    /**
     * Generate a new value from the specified sequence in the database, and return it.
     * This is supported only on RDBMS brands that support sequences
     * (e.g. Oracle, PostgreSQL, DB2).  Other RDBMS brands return null.
     *
     * @param string $sequenceName
     * @return string
     */
    public function nextSequenceId($sequenceName)
    {
        return $this->_master->lastSequenceId($sequenceName);
    }

    /**
     * Returns a list of the tables in the database.
     *
     * @return array
     */
    public function listTables()
    {
        return $this->_master->listTables();
    }

    /**
     * Returns the column descriptions for a table.
     *
     * The return value is an associative array keyed by the column name,
     * as returned by the RDBMS.
     *
     * The value of each array element is an associative array
     * with the following keys:
     *
     * SCHEMA_NAME => string; name of database or schema
     * TABLE_NAME  => string;
     * COLUMN_NAME => string; column name
     * COLUMN_POSITION => number; ordinal position of column in table
     * DATA_TYPE   => string; SQL datatype name of column
     * DEFAULT     => string; default expression of column, null if none
     * NULLABLE    => boolean; true if column can have nulls
     * LENGTH      => number; length of CHAR/VARCHAR
     * SCALE       => number; scale of NUMERIC/DECIMAL
     * PRECISION   => number; precision of NUMERIC/DECIMAL
     * UNSIGNED    => boolean; unsigned property of an integer type
     * PRIMARY     => boolean; true if column is part of the primary key
     * PRIMARY_POSITION => integer; position of column in primary key
     *
     * @param string $tableName
     * @param string $schemaName OPTIONAL
     * @return array
     */
    public function describeTable($tableName, $schemaName = null)
    {
        return $this->_master->describeTable($tableName, $schemaName);
    }

    /**
     * Creates a connection to the database.
     *
     * @return void
     */
    protected function _connect()
    {
        $this->_master->getConnection();
        $this->guardSlave(function(Zend_Db_Adapter_Abstract $slave){
           $slave->getConnection();
        });
    }

    /**
     * Test if a connection is active
     *
     * @return boolean
     */
    public function isConnected()
    {
        if($this->_master->isConnected())
            return true;

        if($this->_slave !== null && $this->_slave->isConnected())
            return true;

        return false;
    }

    /**
     * Force the connection to close.
     *
     * @return void
     */
    public function closeConnection()
    {
        $this->_master->closeConnection();
        $this->guardSlave(function($slave)
        {
           $slave->closeConnection();
        });
    }

    /**
     * Prepare a statement and return a PDOStatement-like object.
     *
     * @param string|Zend_Db_Select $sql SQL query
     * @return Zend_Db_Statement|PDOStatement
     */
    public function prepare($sql)
    {
        $sql_string = strtolower(trim($sql));
        if(strpos($sql_string,'select') === 0 && $this->_slave != null && !$this->_in_transaction)
        {
            return $this->_slave->prepare($sql);
        }

        return $this->_master->prepare($sql);
    }

    /**
     * Gets the last ID generated automatically by an IDENTITY/AUTOINCREMENT column.
     *
     * As a convention, on RDBMS brands that support sequences
     * (e.g. Oracle, PostgreSQL, DB2), this method forms the name of a sequence
     * from the arguments and returns the last id generated by that sequence.
     * On RDBMS brands that support IDENTITY/AUTOINCREMENT columns, this method
     * returns the last value generated for such a column, and the table name
     * argument is disregarded.
     *
     * @param string $tableName   OPTIONAL Name of table.
     * @param string $primaryKey  OPTIONAL Name of primary key column.
     * @return string
     */
    public function lastInsertId($tableName = null, $primaryKey = null)
    {
        $this->_master->lastInsertId($tableName, $primaryKey);
    }

    protected function _quote($value)
    {
        return $this->_master->quote($value);
    }

    public function quote($value, $type = null)
    {
        return $this->_master->quote($value, $type);
    }

    public function getQuoteIdentifierSymbol()
    {
        return $this->_master->getQuoteIdentifierSymbol();
    }

    /**
     * Begin a transaction.
     */
    protected function _beginTransaction()
    {
        $this->_master->_beginTransaction();
    }

    /**
     * Commit a transaction.
     */
    protected function _commit()
    {
        $this->_master->_commit();
    }

    /**
     * Roll-back a transaction.
     */
    protected function _rollBack()
    {
        $this->_master->_rollBack();
    }

    /**
     * Set the fetch mode.
     *
     * @param integer $mode
     * @return void
     * @throws Zend_Db_Adapter_Exception
     */
    public function setFetchMode($mode)
    {
        $this->guardSlave(function($slave) use ($mode)
        {
            $slave->setFetchMode($mode);
        });

        $this->_master->setFetchMode($mode);
    }

    /**
     * Adds an adapter-specific LIMIT clause to the SELECT statement.
     *
     * @param mixed $sql
     * @param integer $count
     * @param integer $offset
     * @return string
     */
    public function limit($sql, $count, $offset = 0)
    {
        return $this->_master->limit($sql, $count, $offset);
    }

    /**
     * Check if the adapter supports real SQL parameters.
     *
     * @param string $type 'positional' or 'named'
     * @return bool
     */
    public function supportsParameters($type)
    {
        $this->_master->supportsParameters($type);
    }

    /**
     * Retrieve server version in PHP style
     *
     * @return string
     */
    public function getServerVersion()
    {
        return $this->_master->getServerVersion();
    }

    protected function guardSlave($function)
    {
        if($this->_slave != null)
        {
            $function($this->_slave);
        }
    }
}
