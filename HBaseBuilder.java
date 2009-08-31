/*
 * Copyright 2003-2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You are receiving this code free of charge, which represents many hours of
 * effort from other individuals and corporations.  As a responsible member 
 * of the community, you are asked (but not required) to donate any 
 * enhancements or improvements back to the community under a similar open 
 * source license.  Thank you. -TMN
 */
package com.enernoc.rnd.shredder.core.groovy;

import groovy.lang.Closure;
import groovy.util.Proxy;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * <p>Code to implement a builder-style DSL in Groovy for the HBase client API.  
 * This provides convenience method as well as providing a more declarative 
 * wrapper for HBase.  The class currently supports wrappers for 
 * {@link #create(String, Closure) creating and modifying} tables, 
 * {@link #update(HTable, Closure) batch updates}, and 
 * {@link #scan(Map, HTable, Closure) scanners}.</p>
 * 
 * <p>Example:
 * <pre>
 * def hbase = HBaseBuilder.connect() // may optionally pass host name
 * 
 * // Create:  this will create a table if it does not exist, or disable
 * & update column families if the table already does exist.  The table will be enabled when the create statement returns
 * hbase.create( 'myTable' ) {
 *  family( 'familyOne' ) {
 *     inMemory = true
 *     bloomFilter = false
 *  }
 *  family 'familyTwo' // relaxed Groovy syntax form without parens and column family defaults
 * }
 * 
 * 
 * // Insert/ update rows:
 * hbase.update( 'myTable' ) {
 *  row( 'rowOne' ) {
 *     family( 'familyOne' ) {
 *          col 'one', 'someValue'
 *          col 'two', 'anotherValue'
 *          col 'three', 1234
 *     }
 *   
 *     // alternate form that doesn't use nested family name:
 *     col 'familyOne:four', 12345
 *  }
 *  row( 'rowTwo' ) { more column values  }
 *  // etc
 * }
 * 
 * 
 * // scan table:
 * hbase.scan( cols : ['familyOne:*'] ) { row ->
 * 	println "Scanning row: ${row.key}"
 * 	println " Column val : ${row.getString('familyOne:one')}"
 * 	
 * 	// return this value to break out of a scan loop
 * 	SCAN_BREAK
 * } 
 * </pre></p>
 * 
 * @author <a href='mailto:tnichols@enernoc.com'>Tom Nichols</a>
 */
public class HBaseBuilder {

	protected Logger log = LoggerFactory.getLogger(getClass());
	/** Return this value to break out of a scan closure execution loop if the 
	 * user wants to end scanning.  Since HBase is in the scan closure scope, 
	 * all that is necessary is to call <code>return SCAN_BREAK</code> */
	public final String SCAN_BREAK = "com.enernoc.rnd.eps.SCANNER_BREAK";

	HBaseConfiguration conf;	
	HBaseAdmin admin;
	
	/** Default table to use for update/ scan operations */
	HTable table;
	
	protected HBaseBuilder() {
		this( new HBaseConfiguration() );
	}
	
	protected HBaseBuilder( HBaseConfiguration conf ) {
		log.info( "Connecting to host: {}", conf.get(HConstants.MASTER_ADDRESS) );
		this.conf = conf;
	}
	
	/**
	 * Connect to HBase using the default configuration (similar to the no-arg
	 * constructor for {@link HBaseAdmin} and {@link HTable}.
	 * @return the HBase builder instance
	 */
	public static HBaseBuilder connect() {
		return new HBaseBuilder();
	}
	
	/**
	 * Connect to the given HBase host address.  
	 * @see HConstants#MASTER_ADDRESS
	 * @param host host name or IP of the master HBase node.
	 * @return the HBase builder instance
	 */
	public static HBaseBuilder connect( String host ) {
		HBaseConfiguration conf = new HBaseConfiguration();
		conf.set( HConstants.MASTER_ADDRESS, host );
		return new HBaseBuilder( conf );
	}

	/**
	 * Connect to HBase using the given configuration.  Analogous to calling
	 * <code>new HTable( conf )</code> and <code>new HBaseAdmin( conf )</code>
	 * @param conf configuration to use for connecting to the HBase server.
	 * @return the HBase builder instance
	 */
	public static HBaseBuilder connect( HBaseConfiguration conf ) {
		return new HBaseBuilder( conf );
	}		
	
	/**
	 * Allows for named arguments to be passed to the <code>connect</code> 
	 * method.  All arguments will be converted into a <code>HBaseConfiguration</code>
	 * instance <strong>except</strong> for the following:
	 * <dl>
	 *  <dt>host</dt><dd>Host name to connect to.  Short for 
	 *    {@link HConstants#MASTER_ADDRESS}.  This value will override 
	 *    any master host address set in a loaded conf file.</dd>
	 *  <dt>table</dt><dd>Default table name to use (see {@link #setTableName(String)})</dd>
	 *  <dt>confURL</dt><dd>Location of an HBase configuration file to load.
	 *    This file is loaded before any extra named parameters are added 
	 *    to the configuration instance, so explicit named parameters passed to 
	 *    this method take preference over what is loaded from the config file.</dd>
	 * </dl>
	 * Example:
	 * <pre>def hbase = HBaseBuilder.connect( host : 'localhost', table: 'myTable', 
	 *                     confURL : 'file:///opt/hbase/conf/myconf.xml',
	 *                     HConstants.HBASE_DIR : '/opt/hbase' )</pre>
	 * @param args
	 * @return the HBase builder instance
	 * @throws IOException if the 'confURL' or tableName param is invalid
	 */
	public static HBaseBuilder connect( Map<String, Object> args ) throws IOException {
		String host = (String)args.remove("host"); 
		String tableName = (String)args.remove("table");
		String confURL = (String)args.remove("confURL");
		
		HBaseConfiguration conf = new HBaseConfiguration();
		if ( confURL != null ) conf.addResource( new URL( confURL ) );
		// pass any remaining args to HBaseConfiguration
		for ( Map.Entry<String, Object> entry : args.entrySet() ) {
			Object val = entry.getValue();
			String key = entry.getKey();
			if ( val == null ) continue;
			else if ( val instanceof Integer ) conf.setInt(key, (Integer)val);
			else if ( val instanceof Long ) conf.setLong(key, (Long)val);
			else if ( val instanceof Boolean ) conf.setBoolean(key, (Boolean)val);
			else if ( val instanceof String[] ) conf.setStrings(key, (String[])val);
			else conf.set( key, val.toString() );
		}
		
		if ( host != null ) conf.set( HConstants.MASTER_ADDRESS, host );
		
		HBaseBuilder hb = new HBaseBuilder( conf );
		hb.setTableName( tableName );
		return hb;
	}
	
	/**
	 * Create an HBase table, or modify the properties of an existing table.  If
	 * the table already exists, it will be disabled for modification.  When 
	 * this method returns, the table will be enabled.   
	 * 
	 * 
	 * @see CreateDelegate
	 * @see HBaseAdmin#createTable(HTableDescriptor)
	 * @param tableName name of the table to create or modify
	 * @param tableConfig closure in which to define table settings as well as 
	 *  calls to family definiton.  The closure should accept a single parameter,
	 *  which will be an {@link HTableDescriptor} instance which will be used 
	 *  to create or modify the table once the closure returns.  If the table 
	 *  already exists, the HTableDescriptor instance will be retrieved from 
	 *  a call to {@link HBaseAdmin#listTables()}.
	 * @return the HTableDescriptor used to define this table.
	 * @throws MasterNotRunningException
	 * @throws IOException thrown by HBaseAdmin
	 */
	public HTableDescriptor create( String tableName, Closure tableConfig ) throws MasterNotRunningException, IOException {
		HTableDescriptor table = null;
		
		if ( getAdmin().tableExists( tableName ) ) {
			for ( HTableDescriptor td : admin.listTables() )
				if ( td.getNameAsString().equals(tableName) ) {
					table = td; break;
				}
			log.info( "Taking table {} offline for modification", tableName );
			
			/* table needs to be disabled before calling the create delegate 
			   where family configuration is done on a live table descriptor */
			if ( admin.isTableEnabled(tableName) ) admin.disableTable( tableName );
		}
		else {
			log.info( "Creating table '{}'...", tableName );
			table = new HTableDescriptor( tableName );
		}
				
		CreateDelegate cd = new CreateDelegate(admin,table);
		tableConfig.setDelegate( cd );
		tableConfig.setResolveStrategy( Closure.DELEGATE_FIRST );
		tableConfig.call( table );
		
		if ( admin.tableExists( tableName ) ) {
			byte[] tableNameBytes = tableName.getBytes();
			List<HColumnDescriptor> existingCols = Arrays.asList( 
					admin.getTableDescriptor(tableName).getColumnFamilies() );
			for ( HColumnDescriptor col : table.getColumnFamilies() ) {
				if ( existingCols.contains(col) ) 
					admin.modifyColumn( tableNameBytes, col.getName(), col );
				else admin.addColumn( tableNameBytes, col );
			}
			/* currently this will not delete existing columns if they are not 
			   in the new table definition (just to be safe) */
			//TODO also update properties on the table?
			log.info( "Updated table: {}", table );
		}
		else {
			admin.createTable( table );
			log.info( "Created table: {}", table );
		}
		if ( ! admin.isTableEnabled( tableName ) ) admin.enableTable( tableName );
		log.debug( "Enabled table: {}", tableName );
		return table;
	}
	
	/**
	 * Perform row updates on the current table.
	 * @see #setTableName(String)
	 * @see #update(HTable, Closure)
	 * @param updateClosure
	 * @return the HTable instance used for updates.
	 * @throws IOException
	 */
	public HTable update( Closure updateClosure ) throws IOException {
		return this.update( this.table, updateClosure );
	}
	
	/**
	 * Short for <code>hbase.update( new HTable(hbase.getConfiguration(), 
	 * tableName), updateClosure );</code>
	 * @param tableName table on which to perform the updates
	 * @param updateClosure code to create row and column updates
	 * @return The HTable instance used for committing the BatchUpdates
	 * @throws IOException if the underlying HTable encounters errors during update
	 */
	public HTable update( String tableName, Closure updateClosure ) throws IOException {
		return this.update( new HTable(conf, tableName), updateClosure );
	}
	
	/**
	 * <p>Update rows in the given table.  The HTable instance will be passed to 
	 * the <code>updateClosure</code>.  Within that closure, helper methods from 
	 * an {@link UpdateDelegate} instance will be available to automatically 
	 * create {@link BatchUpdate}s and set column values.  All BatchUpdates 
	 * are committed after the closure returns.</p>
	 * <p>Example:
	 * <pre>hbase.update( myTable ) {
	 * 	row( '1234' ) { update -> // creates a BatchUpdate instance
	 * 	
	 * 		// helper method that sets the value 1234 on the column 'family:name' on this BatchUpdate
	 * 		col 'family:name', 1234
	 * 
	 *   	// if updating many columns in the same family: 
	 *   	family( 'familyName' ) {
	 *  		// all column updates will be prepended with 'familyName:'
	 *  		col 'col1', 1234
	 *  		col 'col2', 'someString'
	 *  		col 'col3', 
	 * 	}
	 * }</pre></p>
	 * @see UpdateDelegate
	 * @param table table on which to perform the updates
	 * @param updateClosure code to create row and column updates
	 * @return The HTable instance used for committing the BatchUpdates
	 * @throws IOException if the underlying HTable encounters errors during update
	 */
	public HTable update( HTable table, Closure updateClosure ) throws IOException {
		UpdateDelegate delegate = new UpdateDelegate();
		try {
			updateClosure.setDelegate( delegate );
			updateClosure.setResolveStrategy( Closure.DELEGATE_FIRST );
			updateClosure.call( table );
			
			String tableName = new String(table.getTableName());
			List<BatchUpdate> updates = delegate.getUpdates();
			for ( BatchUpdate update : updates ) {
				table.commit( update );
				log.trace( "Row update to table '{}': {}", tableName, update );
			}
			log.debug( "Updated {} rows in table '{}'", updates.size(), tableName );
			
		}
		finally { delegate = null; }
		return table;
	}
	
	/**
	 * Return a single row.  Optional named arguments:
	 * <dl>
	 *   <dt>cols</dt><dd>List of columns to return.  May be a simple regex pattern</dd>
	 *   <dt>versions</dt><dd>numbers of versions</dd>
	 *   <dt>timestamp</dt><dd>Either a <code>Date</code> or <code>long</code></dd>
	 *   <dt>lock</dt><dd>RowLock instance</dd>
	 * </dl>
	 * 
	 * @param args optional named arguments  
	 * @param row required row ID to retrieve
	 * @param table required table to search
	 * @return a {@link RowResultProxy} of the row, or <code>null</code> if the 
	 *  underlying call to {@link HTable#getRow(String) getRow} returned null.
	 * @throws IOException
	 */
	public RowResultProxy getRow( Map<String,Object> args, String row, HTable table ) throws IOException {
		long timestamp =  getTimestamp( args.get("timestamp") );
		
		Object val = args.get("versions");
		Integer versions = val != null ? (Integer)val : 1;
		
		String[] cols = null;
		val = null;
		val = args.get("cols");
		if ( val != null ) { // should be a list
			List<?> list = (List<?>)val;
			cols = new String[list.size()];
			for ( int i=0; i< list.size(); i++ ) cols[i] = list.get(i).toString();
		}
		
		// might be null; this is OK.
		RowLock lock = (RowLock)args.get("lock");
		
		RowResult rr = table.getRow(row,cols,timestamp,versions,lock);
		if ( rr == null ) return null;
		return new RowResultProxy( rr );
	}
	
	/**
	 * See {@link #getRow(Map, String, HTable)}.
	 * @param args optional named arguments  
	 * @param row required row ID to retrieve
	 * @param table required table to search
	 * @return a {@link RowResultProxy} of the row, or <code>null</code> if the 
	 *  underlying call to {@link HTable#getRow(String) getRow} returned null.
	 * @throws IOException
	 */
	public RowResultProxy getRow( Map<String,Object> args, String row, String tableName ) throws IOException {
		return getRow( args, row, new HTable( conf, tableName ) );
	}
	
	/**
	 * See {@link #getRow(Map, String, HTable)}.
	 * @param args optional named arguments  
	 * @param row required row ID to retrieve
	 * @return a {@link RowResultProxy} of the row, or <code>null</code> if the 
	 *  underlying call to {@link HTable#getRow(String) getRow} returned null.
	 * @throws IOException
	 */
	public RowResultProxy getRow( Map<String,Object> args, String row ) throws IOException {
		if ( this.table == null ) throw new IllegalStateException(
				"tableName property must be set before calling methods that do not specify a table name." );
		return getRow( args, row, this.table );
	}

	/**
	 * Short form of {@link #scan(Map,HTable,Closure)}.  This method uses
	 * the default table defined in {@link #setTableName(String)}.  
	 */
	public HTable scan( Map<String,Object> args, Closure scanClosure ) throws IOException {
		return this.scan( args, this.table, scanClosure );
	}
	
	/**
	 * Alternate form of {@link #scan(Map,HTable,Closure)} which creates an 
	 * HTable instance from the <code>tableName</code> string argument.  Note
	 * that for readability, the table name may be given as the first parameter,
	 * i.e. <code>htable.scan( 'myTable', cols: ['col:1'] ) { /* result closure * / }</code>
	 * @see #scan(Map,HTable,Closure)
	 * @param args named parameters for scanning
	 * @param tableName name of the table to scan
	 * @param scanClosure closure to iterate over each row result
	 * @return the table that was scanned
	 * @throws IOException if the underlying HBase API throws an exception
	 */
	public HTable scan( Map<String,Object> args, String tableName, Closure scanClosure ) throws IOException {
		return this.scan( args, new HTable( conf, tableName ), scanClosure );
	}
	
	/**
	 * <p>Create a scanner on the given table passing each {@link RowResult} to 
	 * the <code>scanClosure</code>.  The Scanner is guaranteed to be closed 
	 * when the scanner iteration completes (either normally or due to an 
	 * exception).  Note that each <code>RowResult</code> is actually {@link RowResultProxy wrapped} with additional 
	 * convenience methods when it is passed to the <code>scanClosure</code>.</p>  
	 * 
	 * <p>Valid named arguments are:
	 * <dl>
	 *   <dt>cols</dt><dd>List of columns to return.  If not specified, all columns in all families will be returned.</dd>
	 *   <dt>start</dt><dd>Starting row</dd>
	 *   <dt>end</dt><dd>Ending row (not passed to the closure)</dd>
	 *   <dt>timestamp</dt><dd>Either a <code>Date</code> or <code>long</code></dd>
	 * </dl>
	 * </p>
	 * <p>Note that for readability, the table name may be given as the first 
	 * parameter, i.e. 
	 * <code>htable.scan( 'myTable', cols: ['col:1'] ) { print it['col:1'] }</code>
	 * @see RowResultProxy
	 * @param args named parameters for scanning
	 * @param table table to scan
	 * @param scanClosure closure to iterate over each row result
	 * @return the table that was scanned
	 * @throws IOException if the underlying HBase API throws an exception
	 */
	public HTable scan( Map<String,Object> args, HTable table, Closure scanClosure ) throws IOException {
		// TODO this might throw a CCE if value is a GString rather than a String...
		List<String> cols = (List<String>)args.get("cols"); 
		byte[][] colArray;
		if ( cols == null ) { // get all columns in all families
			Collection<HColumnDescriptor> cds = table.getTableDescriptor().getFamilies();
			colArray = new byte[cds.size()][];
			int i=0;
			for ( HColumnDescriptor cd : cds ) {
				log.debug( "Scanning all columns in family: {}", cd.getNameAsString() );
				colArray[i++] = cd.getNameWithColon();
			}
		}
		else colArray = Bytes.toByteArrays(cols.toArray(new String[cols.size()]) );
		
		long timestamp = getTimestamp( args.get("timestamp") );
		
		Object tempVal = args.get("start");
		byte[] startRow = tempVal instanceof byte[] ? (byte[])tempVal :
			tempVal != null ? Bytes.toBytes( tempVal.toString() ) : 
				HConstants.EMPTY_START_ROW;
		
		tempVal = args.get("end");
		byte[] endRow =  tempVal instanceof byte[] ? (byte[])tempVal :
			tempVal != null ? Bytes.toBytes( tempVal.toString() ) : 
				null;  // endRow may be null; this is OK
		
		Scanner scanner = endRow == null ?
				table.getScanner( colArray, startRow, timestamp ) 
				: table.getScanner( colArray, startRow, endRow, timestamp );
		
		scanClosure.setDelegate( this );
				
		int rowCount = 0;
		long ts = System.currentTimeMillis();
		try {
			Object result = null;
			Proxy rowProxy = new RowResultProxy();
			while ( result != SCAN_BREAK ) {
				RowResult row = scanner.next();
				if ( row == null ) break;
				rowCount ++;
				rowProxy.setAdaptee( row );
				result = scanClosure.call( rowProxy );
			}
		}
		finally { 
			scanner.close();
			ts = System.currentTimeMillis() - ts;
			if ( log.isDebugEnabled() ) {
				log.debug( "Scanned {} rows on table '{}' in {}ms", 
					new Object[] {rowCount, new String(table.getTableName()), ts} );
			}
		}
		return table;
	}
	
	/**
	 * Can convert Date and Calendar instances to Byte[] for storage, as well as
	 * those supported by {@link Bytes}.  Note that Calendar timzone and locale 
	 * are not preserved.  (Calendar and Date are both simply converted to longs). 
	 * @param val value to convert
	 * @return byte[] suitable for storage by HBase
	 * @throws IllegalArgumentException if the argument value is not a 
	 * 	convertible type
	 */
	public static byte[] getBytes( Object val ) throws IllegalArgumentException {
		if ( val == null ) return null;
		if ( val.getClass() == String.class ) return Bytes.toBytes((String)val);
		if ( val.getClass() == Integer.class ) return Bytes.toBytes((Integer)val);
		if ( val.getClass() == Long.class ) return Bytes.toBytes((Long)val);
		//TODO Double
		
		if ( val instanceof Date ) return Bytes.toBytes(((Date)val).getTime());
		if ( val instanceof Calendar ) return Bytes.toBytes(((Calendar)val).getTime().getTime());
		
		throw new IllegalArgumentException("Value must be either a String, Number, Date or Calendar");
	}
	
	/**
	 * Create a timestamp from the given object.  The object may be an int, 
	 * long, Date or Calendar.
	 * @param ts value to convert 
	 * @return a suitable HBase timestamp value
	 * @throws IllegalArgumentException if an object of the wrong type is given.
	 */
	public static long getTimestamp( Object ts ) throws IllegalArgumentException {
		if ( ts == null ) return HConstants.LATEST_TIMESTAMP;
		if ( ts.getClass() == Long.class || ts.getClass() == Integer.class ) return (Long)ts;
		
		if ( ts instanceof Date ) return ((Date)ts).getTime();
		if ( ts instanceof Calendar ) return ((Calendar)ts).getTime().getTime();

		throw new IllegalArgumentException("Timestamp must be either a long, Date or Calendar");
	}
	
	/**
	 * @return the configuration used by this instance to connect to HBase
	 */
	public HBaseConfiguration getConfiguration() {
		return this.conf;
	}
	
	/**
	 * @return HBaseAdmin instance used when creating or modifying table 
	 * structures. An HBaseAdmin instance is normally not created until 
	 * required by a call to <code>create(tableName) {...}</code>.  This
	 * method will force an HBaseAdmin instance to be created if it has not
	 * already.  The underlying {@link getConfiguration() HBaseConfiguration}
	 * is passed to the HBaseAdmin instance.
	 * @return the HBaseAdmin instance that will be used for calls to 
	 *  <code>create(..)</code>
	 * @throws MasterNotRunningException
	 */
	public HBaseAdmin getAdmin() throws MasterNotRunningException {
		if ( this.admin == null ) this.admin = new HBaseAdmin( this.conf );
		return this.admin;
	}
	
	/** Set the name of the 'default' table, i.e. when no table name is given 
	 * in <code>update</code> and <code>scan</code> operations. 
	 * @throws IOException if the table name is not a valid table in the current
	 * HBase server. 
	 */
	public void setTableName( String name ) throws IOException {
		if ( name == null || name.length() < 1 ) {
			this.table = null;
			return;
		}
		setTable( new HTable( this.conf, name ) );
	}
	
	/**
	 * Set the default table used for all methods that do not accept a 'table'
	 * or 'tableName' parameter. 
	 * @param table
	 */
	public void setTable( HTable table ) {
		this.table = table;
	}
	


	/**
	 * This class is used as the {@link Closure} delegate to add the 
	 * <code>family('familyName')</code> method in the scope of the 
	 * {@link #create(String, Closure)} method's closure. 
	 */
	public class CreateDelegate {
		protected HBaseAdmin admin;
		protected HTableDescriptor table;
		
		/** Called internally by the HBase builder */ 
		protected CreateDelegate(HBaseAdmin admin, HTableDescriptor table) {
			this.admin = admin; this.table = table;
		}
		
		/**
		 * Create a column family with all default options
		 * @param familyName family name to create
		 * @return the column family definition that was created
		 * @throws IOException if any errors were thrown by the HBase API
		 */
		public HColumnDescriptor family( String familyName ) throws IOException {
			return this.family( familyName, null );
		}

		public HColumnDescriptor family( String familyName, Closure familyConfig ) throws IOException {
			// column family names must end w/ a colon.
			if ( ! familyName.endsWith(":") ) familyName += ':';
		
			HColumnDescriptor colFamily = table.getFamily(familyName.getBytes());
			if ( colFamily == null ) colFamily = new HColumnDescriptor(familyName);

			// Allow closure arg to configure column family options:
			if ( familyConfig != null ) {
				familyConfig.setDelegate( colFamily );
				familyConfig.setResolveStrategy( Closure.DELEGATE_ONLY );
				familyConfig.call();
			}

			// Add column to table.
			if ( ! table.hasFamily( familyName.getBytes() ) ) table.addFamily( colFamily );
			else admin.modifyColumn( table.getNameAsString(), familyName, colFamily );
			return colFamily;
		}
	}

	/**
	 * This class is used as the {@link Closure} delegate to add the 
	 * <code>family('familyName')</code> and <code>col( name, val)</code> 
	 * methods in the scope of the {@link HBase#update(HTable, Closure)} 
	 * method's closure argument. 
	 */
	public class UpdateDelegate {
		 // list of BatchUpdates created from calls to row('id') {....}
		private List<BatchUpdate> updates = new ArrayList<BatchUpdate>();
		
		/**
		 * List of updates created from calls to {@link #row(String, Closure)}.
		 * Each <code>row(..)</code> call creates a new BatchUpdate which is 
		 * then appended to this list when the call returns.
		 * @return
		 */
		public List<BatchUpdate> getUpdates() { return this.updates; }
		
		/**
		 * Current update instance, available for direct access within each 'row'
		 * call.  This will be null outside of any row closure.
		 */
		protected BatchUpdate currentUpdate;
		
		/** Set by the call to {@link #family(String, Closure)}.  Within the 
		 * family closure, this will be set to the family name argument. */
		protected String currentFamily = null;
		
		/**
		 * User-set timestamp that should be set at the beginning of the 
		 * update { ... } closure.  If this is not explicitly set it will
		 * default to HConstants.LATEST_TIMESTAMP, which is what BatchUpdate 
		 * defaults to. 
		 */
		Object defaultTimestamp = HConstants.LATEST_TIMESTAMP;
		
		/**
		 * Short form for row( rowName, timestamp, rowClosure ) which uses the 
		 * default timestamp.
		 */
		public void row( String rowName, Closure rowClosure ) {
			row( rowName, this.defaultTimestamp, rowClosure );
		}
		
		/**
		 * Method called within the 'update' closure in order to set values
		 * for each row being inserted or updated.  This method takes a timestamp 
		 * parameter that is used for all values set in this row update.  The 
		 * parameter may be of type long, Date or Calendar.  The current 
		 * BatchUpdate instance is also passed as a parameter to the rowClosure.
		 * @see HBase#getTimestamp(Object)
		 * @param rowName the row key
		 * @param timestamp Date, Calendar or long timestamp to be used for this row update
		 * @param rowClosure closure used to set column values via calls to 'family',
		 *   'col' or direct use of the 'currentUpdate' BatchUpdate property.
		 */
		public void row( String rowName, Object timestamp, Closure rowClosure ) {
			if ( this.currentUpdate != null ) throw new IllegalStateException("Cannot nest row calls");
		
			this.currentUpdate = new BatchUpdate( rowName );
			
			rowClosure.setDelegate( this );
			rowClosure.setResolveStrategy( Closure.DELEGATE_FIRST );
			rowClosure.call( currentUpdate );
			
			currentUpdate.setTimestamp( HBaseBuilder.getTimestamp( timestamp ) );
			this.updates.add( this.currentUpdate );
			this.currentUpdate = null; // 'currentUpdate' should not be available outside of row closure
		}
		
		/**
		 * Method closure used within the row closure to insert several columns in
		 * the same column family. All calls to <code>col( 'name', 'val' )</code>
		 * within this closure will have the enclosing family prepended to each 
		 * column name.  
		 * 
		 * @param familyName family name to automatically use within the colClosure 
		 *   scope.  The family name will automatically be appended with ':' if
		 *   not explicitly supplied.
		 * @param colClosure all <code>col</code> calls within this closure will 
		 *   automatically be prepended with this family name.
		 */
		public void family(String familyName, Closure colClosure ) {
			if ( currentFamily != null ) throw new IllegalStateException("Cannot nest family calls");
			if ( currentUpdate == null ) throw new IllegalStateException("Family must be called from within a row closure");
		
			currentFamily = familyName.endsWith(":") ? familyName : (familyName + ':');
			colClosure.setDelegate( this );
			colClosure.setResolveStrategy( Closure.DELEGATE_FIRST );
			colClosure.call(); //currentUpdate
			currentFamily = null;
		}
		
		/**
		 * Method call to set an individual column value.  This may be called 
		 * directly from a row closure, or from a nested family closure.  If it 
		 * is called directly from the row closure,  a family-qualified column 
		 * name must be given (i.e. <code>col( 'family:name', val )</code>.  If
		 * this is called from a family closure, only the column name should be 
		 * given, i.e. <code>col( 'name', val )</code>.
		 * @see HBase#getBytes(Object) 
		 */
		public void col( String columnName, Object val ) {
			if ( currentUpdate == null ) throw new IllegalStateException( 
					"Col must be called from within a row or family closure" );
			if ( currentFamily != null ) columnName = currentFamily + columnName;
			currentUpdate.put( columnName, HBaseBuilder.getBytes(val) ) ;
		}
	}
	
	/**
	 * This wraps {@link RowResult} instances that are passed to the closure
	 * call in {@link HBase#scan(Map, HTable, Closure) HBase.scan(..)}.  This
	 * class essentially extends RowResult; Groovy's method dispatch will pass
	 * any method calls to the RowResult instance that do not match the 
	 * signature of a method on this class.
	 */
	public class RowResultProxy extends Proxy implements Iterable<CellResult> {
		
		public RowResultProxy() { super(); }
		
		public RowResultProxy( RowResult rr ) {
			super();
			this.setAdaptee( rr );
		}
		
		/**
		 * Convenience iterator to traverse the set of column keys, assuming
		 * each key is a String.
		 */
		@Override public Iterator<CellResult> iterator() {
			return new Iterator<CellResult>() {
				Iterator<byte[]> iter = getRow().keySet().iterator();
				RowResult row = getRow();

				@Override public CellResult next() { 
					byte[] key = iter.next();
					return new CellResult( key, row.get(key) ); 
				}

				@Override public boolean hasNext() { return iter.hasNext(); }
				@Override public void remove() { iter.remove(); }
			};
		}
		
		public CellResult getAt( String key ) {
			return getAt( Bytes.toBytes( key ) );
		}
		
		public CellResult getAt( byte[] bKey ) {
			return new CellResult( bKey, getRow().get( bKey ) );	
		}
		
		/** 
		 * Returns the underlying RowResult instance.  Note that direct access
		 * to this instance is probably unnecessary as any RowResult method
		 * calls to the proxy are automatically delegated to this underlying
		 * instance.
		 * @return the proxied {@link RowResult} instance.
		 */  
		public RowResult getRow() { return (RowResult)getAdaptee(); }
		
		/** Convenience method to convert the row key to a string */
		public String getKey() {
			return Bytes.toString( getRow().getRow() );
		}
		
		/** Convenience method to convert row key to a long */
		public long getKeyAsLong() {
			return Bytes.toLong( getRow().getRow() );
		}
		
		/**
		 * Retrieve the given column value for this row as a String
		 */
		public String getString( String col ) {
			return Bytes.toString( getRow().get( col.getBytes() ).getValue() );
		}
		
		/**
		 * Retrieve the given column value for this row as an Int
		 */
		public int getInt( String col ) {
			return Bytes.toInt( getRow().get( col.getBytes() ).getValue() );
		}
		
		/**
		 * Retrieve the given column value for this row as a Long
		 */
		public long getLong( String col ) {
			return Bytes.toLong( getRow().get( col.getBytes() ).getValue() );
		}
		
		/**
		 * Retrieve the given column value for this row as a Date
		 */
		public Date getDate( String col ) {
			return new Date( getLong(col) );
		}
		
		/**
		 * Retrieve the given column value for this row as a String
		 * TODO implement when Bytes.toDouble is implemented 
		 */
		public double getDouble( String col ) {
			throw new UnsupportedOperationException( "Not yet supported" );
			//return Bytes.toDouble( getRow().get( col.getBytes() ).getValue() );
		}
	}
	
	/**
	 * Wraps a {@link Cell} in a number of convenience methods, as well as 
	 * retaining the column key which is associated with it.  
	 */
	public class CellResult {
		byte[] name;
		Cell cell;
		public CellResult( byte[] key, Cell cell ) {
			this.name = key; this.cell = cell;
		}
		
		public byte[] getKey() { return this.name; }
		
		public String getName() { return Bytes.toString(name); }
		
		public Cell getCell() { return this.cell; }
		
		public byte[] getValue() { return this.cell.getValue(); }
		
		public String getString() { return Bytes.toString( getValue() ); }
		public int getInt() { return  Bytes.toInt( getValue() ); }
		public long getLong() { return Bytes.toLong( getValue() ); }
		public Date getDate() { return new Date( getLong() ); }

		/** 
		 * Return the timestamp of the underlying cell 
		 * @see Cell#getTimestamp()
		 */
		public long getTimestamp() { return cell.getTimestamp(); }
	}
}
