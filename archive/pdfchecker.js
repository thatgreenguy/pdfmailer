// pdfchecker.js  : Check Jde Job Control table looking for any recently generated Pdf files that are configured 
//                : in JDE to be eligible for Email delivery.
// Author         : Paul Green
// Dated          : 2015-09-03
//
// Synopsis
// --------
//
// Called periodically by pdfmailer.js
// It checks the Jde Job Control Audit table looking for recently completed UBE reports.
// New PDF files are cross checked against JDE email configuration and and if email delivery is required then the report
// is sent to all configured recipients


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
  begin = null;


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
module.exports.queryJdeJobControl = function( 
  dbCn, chkDate, chkTime, pollInterval, hostname, lastPdf, performPolledProcess ) {

  var auditTimestamp,
  query,
  result,
  jdeDate,
  jdeTime,
  jdeDateToday,
  wkAdt;

  begin = new Date();
  log.debug( 'Begin Checking : ' + begin + ' - Looking for new Jde Pdf files since last run' );

  // Checking of JDE Database Job Control is driven by passed Date and Time.
  // On startup where process has not run for a while this date and time could be from several days ago.
  // On subsequent calls (here) this date and time should be from a few seconds ago
  // i.e. first time could be looking for jobs going back several days that require processing but once
  // running it should be looking back and checking for the polling interval only

  // Get todays date from System in JDE Julian format
  jdeDateToday = audit.getJdeJulianDate();

  // If Passed check from Date is not today or we have just crossed midnight threshold the query should not use time 
  // as part of selection.

  if ( jdeDateToday == chkDate ) {
       
        query = "SELECT jcfndfuf2, jcactdate, jcacttime, jcprocessid FROM testdta.F556110 ";
        query += " WHERE jcjobsts = 'D' AND jcfuno = 'UBE' AND jcactdate >= ";
        query += chkDate + ' AND jcacttime >= ' + chkTime;
        query += " AND RTRIM( SUBSTR(jcfndfuf2, 0, (INSTR(jcfndfuf2, '_') - 1)), ' ') in ( SELECT RTRIM(crpgm, ' ') FROM testdta.F559890 WHERE crcfgsid = 'PDFMAILER') ";
        query += " ORDER BY jcactdate, jcacttime";
    	
	log.debug( 'Check Date matches Todays Date : ' + jdeDateToday + ' see: ' + chkDate);

    } else { 
       
        query = "SELECT jcfndfuf2, jcactdate, jcacttime, jcprocessid FROM testdta.F556110 ";
        query += " WHERE jcjobsts = 'D' AND jcfuno = 'UBE' AND jcactdate >= ";
        query += chkDate;
        query += " AND RTRIM( SUBSTR(jcfndfuf2, 0, (INSTR(jcfndfuf2, '_') - 1)), ' ') in ( SELECT RTRIM(crpgm, ' ') FROM testdta.F559890 WHERE crcfgsid = 'PDFMAILER') ";
        query += " ORDER BY jcactdate, jcacttime";
    	
	log.debug( 'Check Date is not today : ' + jdeDateToday + ' see: ' + chkDate);
    }

    log.debug(query);

    dbCn.execute( query, [], { resultSet: true }, function( err, rs ) {
        if ( err ) { 
          log.error( err.message );
          return;
        }

        processResultsFromF556110( 
          dbCn, rs.resultSet, numRows, begin, pollInterval, hostname, lastPdf, chkDate, chkTime, performPolledProcess );

    }); 
}


// Process results of query on JDE Job Control file 
function processResultsFromF556110( 
  dbCn, rsF556110, numRows, begin, pollInterval, hostname, lastPdf, chkDate, chkTime, performPolledProcess ) {

  var jobControlRecord,
  finish;

  rsF556110.getRows( numRows, function( err, rows ) {
    if ( err ) { 
      oracleResultsetClose( dbCn, rsF556110 );
      log.debug("rsF556110 Error");
      return;
	
    } else if ( rows.length == 0 ) {
      oracleResultsetClose( dbCn, rsF556110 );
      finish = new Date();
      log.verbose( 'End Check: ' + finish  + ' took: ' + ( finish - begin ) + ' milliseconds, Last Pdf: ' + lastPdf );
 
      // No more Job control records to process in this run - this run is done - so schedule next run
      performPolledProcess();

    } else if ( rows.length > 0 ) {

      jobControlRecord = rows[ 0 ];
      log.debug( jobControlRecord );

      // Process PDF entry
      processPdfEntry( dbCn, rsF556110, begin, jobControlRecord, pollInterval, hostname, lastPdf, chkDate, chkTime, performPolledProcess );            

    }
  }); 
}

// Called to handle processing of first and subsequent 'new' PDF Entries detected in JDE Output Queue  
function processPdfEntry( dbCn, rsF556110, begin, jobControlRecord, pollInterval, hostname, lastPdf, chkDate, chkTime, performPolledProcess ) {

  var cb = null,
    currentPdf;

  currentPdf = jobControlRecord[ 0 ];
  log.debug('Last PDF: ' + lastPdf + ' currentPdf: ' + currentPdf );

  // If Last Pdf is same as current Pdf then nothing changed since last check
  if ( lastPdf !== currentPdf ) {

    // Process second and subsequent records.
    cb = function() { processLockedPdfFile( dbCn, jobControlRecord, hostname ); }
    lock.gainExclusivity( dbCn, jobControlRecord, hostname, cb );		
  }

  // Process subsequent PDF entries if any - Read next Job Control record
  processResultsFromF556110( dbCn, rsF556110, numRows, begin, pollInterval, hostname, lastPdf, chkDate, chkTime, performPolledProcess );

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
             log.verbose( 'JDE PDF ' + record[0] + ' - Processing Started' );

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
