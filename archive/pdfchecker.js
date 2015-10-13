// pdfchecker.js  : Check Jde Job Control table looking for any recently generated Pdf files that are configured 
//                : in JDE to be eligible for Email delivery.
// Author         : Paul Green
// Dated          : 2015-09-03
//
// Synopsis
// --------
//
// Check the F559811 Jde Pdf Process Queue for records at status ready for emailing
// For each entry fetch the email configuration and send an email with the report attached.
// Shuffle the entry to the next status or 999 Pdf Processing complete


var oracledb = require( 'oracledb' ),
  lock = require( './common/lock.js' ),
  log = require( './common/logger.js' ),
  audit = require( './common/audit.js' ),
  mail = require( './common/mail.js' ),
  async = require( 'async' ),
  exec = require( 'child_process' ).exec,
  dirRemoteJdePdf = process.env.DIR_JDEPDF,
  dirLocalJdePdf = process.env.DIR_SHAREDDATA,
  numRows = 1,
  begin = null,
  hostname = process.env.HOSTNAME;


// Functions -
//
// module.exports.queryJdeJobControl( dbCn, record, begin, pollInterval, hostname, lastPdf, performPolledProcess )
// function processResultsFromF556110( dbCn, rsF556110, numRows, begin, pollInterval, hostname, lastPdf, performPolledProcess )
// function processPdfEntry( dbCn, rsF556110, begin, jobControlRecord, pollInterval, hostname, lastPdf, performPolledProcess )
// function processLockedPdfFile(dbCn, record, hostname )
// function processPDF( record, hostname )
// function passParms(parms, cb)
// function createAuditEntry( parms, cb )
// function removeLock( record, hostname )
// function oracleResultsetClose( dbCn, rs )
// function oracledbCnRelease( dbCn )


// Query the JDE Job Control Master file to fetch all PDF files generated since last audit entry
// Only select PDF jobs that are registered for emailing
module.exports.queryJdePdfProcessQueue = function( dbCn, status, nextStatus, scheduleNextPolledProcess ) {

  var auditTimestamp,
  query,
  result,
  jdeDate,
  jdeTime,
  jdeDateToday,
  wkAdt;

  // Default Email action status is 200, default next status is 999 Complete
  if ( typeof( status ) === 'undefined' ) status = '100';
  if ( typeof( nextStatus ) === 'undefined' ) status = '999';

  begin = new Date();
  log.debug( 'Begin Checking : ' + begin + ' - Check Process Queue for entries that need to be emailed' );


    query = "SELECT jpfndfuf2, jpblkk FROM testdta.F559811 WHERE jpyexpst = '100' "; 
    query += " ORDER BY jpupmj, jpupmt";
    	
    log.debug(query);

    dbCn.execute( query, [], { resultSet: true }, function( err, rs ) {
        if ( err ) { 
          log.error( err.message );
          return;
        }

        processResultsFromF559811( dbCn, rs.resultSet, numRows, begin, scheduleNextPolledProcess );

    }); 
}


// Process results of query on JDE PDF Process Queue 
function processResultsFromF559811( dbCn, rsF559811, numRows, begin, scheduleNextPolledProcess ) {

  var queueRecord,
  finish;

  rsF559811.getRows( numRows, function( err, rows ) {
    if ( err ) { 
      oracleResultsetClose( dbCn, rsF559811 );
      log.debug("rsF559811 Error");
      return scheduleNextPolledProcess( err );;
	
    } else if ( rows.length == 0 ) {
      oracleResultsetClose( dbCn, rsF559811 );
      finish = new Date();
      log.verbose( 'End Check: ' + finish  + ' took: ' + ( finish - begin ) + ' milliseconds, Last Pdf: ' + lastPdf );
 
      // No more records to process in this run - this run is done - so schedule next run
      return scheduleNextPolledProcess( null );;

    } else if ( rows.length > 0 ) {

      queueRecord = rows[ 0 ];
      log.debug( queueRecord );

      // Process PDF entry
      processPdfEntry( dbCn, rsF559811, begin, queueRecord, scheduleNextPolledProcess );            

    }
  }); 
}
 
// Called to handle processing of first and subsequent 'new' PDF Entries detected in JDE Output Queue  
function processPdfEntry( dbCn, rsF559811, begin, queueRecord, scheduleNextPolledProcess ) {

  var cb = null,
    currentPdf;

  currentPdf = queueRecord[ 0 ];

  // Process second and subsequent records.
  cb = function() { processLockedPdfFile( dbCn, queueRecord, hostname ); }
  lock.gainExclusivity( dbCn, queueRecord, cb );		


  // Process subsequent PDF entries if any - Read next Job Control record
  processResultsFromF559811( dbCn, rsF559811, numRows, begin, scheduleNextPolledProcess );

}


// Called when exclusive lock has been successfully placed to process the PDF file
function processLockedPdfFile( dbCn, record, hostname ) {

    var query,
        countRec,
        count,
        cb = null;

    log.verbose( 'JDE PDF ' + record[ 0 ] + " - Lock established" );

    // Check this PDF file has definitely not yet been processed by any other pdfmailer instance
    // that may be running concurrently

    query = "SELECT COUNT(*) FROM testdta.F559849 WHERE pafndfuf2 = '";
    query += record[0] + "'";

    dbCn.execute( query, [], { }, function( err, result ) {
        if ( err ) { 
            log.debug( err.message );
            return;
        };

        countRec = result.rows[ 0 ];
        count = countRec[ 0 ];
        if ( count > 0 ) {
            log.verbose( 'JDE PDF ' + record[ 0 ] + " - Already Processed - Releasing Lock." );
            lock.removeLock( dbCn, record, hostname );

        } else {

             log.warn( 'JDE PDF ' + record[0] + ' - Processing Started' );

             // This PDF file has not yet been processed and we have the lock so process it now.
             // Note: Lock will be removed if all process steps complete or if there is an error
             // Last process step creates an audit entry which prevents file being re-processed by future runs 
             // so if error and lock removed - no audit entry therefore file will be re-processed by future run (recovery)	
             
             processPDF( dbCn, record, hostname ); 

        }
    }); 
}


// Exclusive use / lock of PDF file established so free to process the file here.
function processPDF( dbCn, record, hostname ) {

    var jcfndfuf2 = record[ 0 ],
        jcactdate = record[ 1 ],
        jcacttime = record[ 2 ],
        jcprocessid = record[ 3 ],
        genkey = jcactdate + " " + jcacttime,
        parms = null;

    // Make parameters available to any function in series
    parms = { "dbCn": dbCn, "jcfndfuf2": jcfndfuf2, "record": record, "genkey": genkey, "hostname": hostname };

    async.series([
        function ( cb ) { passParms( parms, cb ) }, 
        function ( cb ) { emailReport( parms, cb ) }, 
        function ( cb ) { createAuditEntry( parms, cb ) }
        ], function(err, results) {

             var prms = results[ 0 ];

             // Lose lock regardless whether PDF file proceesed correctly or not
             removeLock( dbCn, record, hostname );

             // log results of Pdf processing
             if ( err ) {
               log.error("JDE PDF " + prms.jcfndfuf2 + " - Processing failed - check logs in ./logs");
	     } else {
               log.info("JDE PDF " + prms.jcfndfuf2 + " - Mail Processing Complete");
             }
           }
    );
}


// Ensure required parameters for releasing lock are available in final async function
// Need to release lock if PDF file processed okay or failed with errors so it can be picked up and recovered by future runs!
// For example sshfs dbCn to remote directories on AIX might go down and re-establish later
function passParms(parms, cb) {

  cb( null, parms);  

}


function emailReport( parms, cb ) {

  // Email report
  log.verbose( "JDE PDF " + parms.jcfndfuf2 + " - Mailing JDE Report" );

  mail.prepMail( parms.dbCn, parms.jcfndfuf2, cb );    

}


function createAuditEntry( parms, cb ) {

  // Create Audit entry for this Processed record - once created it won't be processed again
  audit.createAuditEntry( parms.dbCn, parms.jcfndfuf2, parms.genkey, parms.hostname, "PROCESSED - MAIL" );
  log.verbose( "JDE PDF " + parms.jcfndfuf2 + " - Audit Record written to JDE" );
  cb( null, "Audit record written" );
}


function removeLock( dbCn, record, hostname ) {

  log.debug( 'removeLock: Record: ' + record + ' for host: ' + hostname );

  lock.removeLock( dbCn, record, hostname );
  log.verbose( 'JDE PDF ' + record[ 0 ] + ' - Lock Released' );
   
}


// Close Oracle database result set
function oracleResultsetClose( dbCn, rs ) {

  rs.close( function( err ) {
    if ( err ) {
      log.error( "Error closing dbCn: " + err.message );
      oracledbCnRelease(); 
    }
  }); 
}


// Close Oracle database dbCn
function oracledbCnRelease( dbCn ) {

  dbCn.release( function ( err ) {
    if ( err ) {
      log.error( "Error closing dbCn: " + err.message );
    }
  });
}
