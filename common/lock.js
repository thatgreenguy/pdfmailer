// Module		: lock.js
// Description		: Common PDF file locking related functions.
// Author		: Paul Green
// Dated		: 2015-08-04
//
// If running multiple docker container apps need a way to ensure a Pdf file is only processed by
// one application hence simple locking strategy employed here.

  
var oracledb = require('oracledb'),
  audit = require( './audit.js' ),
  log = require( './logger.js' ),
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME};


// Insert lock file entry for given PDF if returns okay then caller has exclusive use of PDF file.
// Expect valid JDE Job Control record to be passed along with callback function to process PDF if lock successful

exports.gainExclusivity = function( record, hostname, conn, processLockedPdfFile ) {

  if ( typeof( record ) === 'undefined') { log.error( 'Valid JDE Job Control Record Expected.' ); return; }
  if ( typeof( processLockedPdfFile ) !== 'function') { log.error( 'Callback function expected to process PDF file.' ); return; }
  
  var jcfndfuf2 = record[0],
    jcprocessid = record[3],
    dt = new Date(),
    timestamp = audit.createTimestamp(dt),
    jdetime = audit.getJdeAuditTime(dt),
    jdedate = audit.getJdeJulianDate(dt),
    jdetime = audit.getJdeAuditTime(dt),
    query;

  oracledb.getConnection( credentials, function(err, connection) {
    if ( err ) { 
      log.error( 'Oracle DB Connection Failure : ' + err );
      return;
    }
  
    query = "INSERT INTO testdta.F559858 VALUES (:lkfndfuf2, :lksawlatm, :lkactivid, :lkpid, :lkjobn, :lkuser, :lkupmj, :lkupmt)";
    log.debug( query );

    connection.execute( query, [jcfndfuf2, timestamp, hostname, 'PDFHANDLER', 'CENTOS', 'DOCKER', jdedate, jdetime ], { autoCommit: true }, function( err, result ) {
      if ( err ) {
        log.debug( 'Oracle DB Insert Lock Failure : ' + err.message );
        return;
      }
      connection.release( function( err ) {
        if ( err ) {
          log.error( err.message );
          return;
	}
      });

      // Inserted without error so lock in place - safe to process this PDF file
      processLockedPdfFile( conn, record );
    });
  });
}


// Remove lock file entry for given PDF once all processing completed.
// Expect valid JDE Job Control record to be passed

exports.removeLock = function( record, hostname ) {

  if ( typeof( record ) === 'undefined' ) {
    log.error( 'Expected valid JDE Job Control Record to be passed to remove lock.' );
    return;
  }

  var jcfndfuf2 = record[ 0 ],
    query,
    jcprocessid = record[ 3 ],
    dt = new Date(),
    timestamp = audit.createTimestamp( dt ),
    jdetime = audit.getJdeAuditTime( dt ),
    jdedate = audit.getJdeJulianDate( dt ),
    jdetime = audit.getJdeAuditTime( dt );

  oracledb.getConnection( credentials, function( err, connection ) {
    if ( err ) {
      log.error( 'Oracle DB Connection Failure' + err );
      return;
    }
  
    query = "DELETE FROM testdta.F559858 WHERE lkfndfuf2 = '" + jcfndfuf2  +"' AND lkactivid = '" + hostname + "'";
    log.debug( query );

    connection.execute( query, [ ], { autoCommit: true }, function( err, result ) {
      if ( err ) {
        log.debug( 'Oracle DB Delete Lock failure : ' + err );
        return;
      }
      connection.release( function(err) {
        if ( err ) {
          log.error( 'Oracle DB Connection release error : ' + err.message );
          return;
        }
      });
    });
  });
}
