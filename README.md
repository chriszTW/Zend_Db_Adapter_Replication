Zend_Db_Adapter_Replication
===========================

Zend_Db Adapter that enables a Master/Slave pair to be used by the Application.  Anything I could find made you 'choose', they were basic structures that let you store connections and get connections.  So you literally had to go to everyplace you wanted to use the functionality and get the specific DB object.  I wanted it to be transparent with the least amount of change necessary.  At this point there isn't a way to even get a specific connection as I've not needed it. 

This adapter takes care of knowing whether the Slave can be used or if the Master must be based on the methods called and the SQL used.

The master will be used for all write operations while the slave will be used for most read operations. If you start a transaction, it forces things to the master.

Example
===========================
    // Create 2 connections 'as usual', whatever that is for your project
    
    // connect to the master database
    $master_config = $config->master->toArray();
    
    $master_db = Zend_Db::factory($master_config->type, $master_config);
	  $master_db->getConnection();
    
    // connect to the slave database
    $slave_config = $config->slave->toArray();

    $slave_db = Zend_Db::factory($slave_config->type, $slave_config);
    $slave_db->getConnection();

    $db = new Pictometry_Db_Adapter_Replication($master_db, $slave_db);
    
    ...
    // continue on using $db just as you do a standard Zend_Db_Adapter_* object
    
This has only been tested using Mysqli connections but in theory works with any connection.

License
=======================
MIT License
Please see LICENSE.txt
    