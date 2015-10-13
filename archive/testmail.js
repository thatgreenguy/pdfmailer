//          contentType: "multipart/mixed"
//          filePath: "/src/R5542565_FRZS5M10A_182678_PDF"



var nodemailer = require('nodemailer');

// create re-usable transporter object using SMTP transport
var smtpTransport = nodemailer.createTransport("SMTP", {
	host: '172.31.3.15',
	port: 25
});

// Set-up Email 
var mailOptions = {
	from: "no.reply@dlink.com",
	to: "paul.green@dlink.com",
	subject: "Hi - this is a test email from Node on Centos",
	text: "Hello - Testing Testing 1 2 3 ...",
	html: "<P>Hello - Testing Testing 1 2 3 ...",
        attachments: [{
          filename: "R5542565_FRZS5M10A_182678_PDF.pdf",
          filePath: "/home/pdfdata/R5542565_FRZS5M10A_182678_PDF"
        }]
}

// Send Email
smtpTransport.sendMail(mailOptions, function(error, response) {
	if (error) {
		console.log(error);
	} else {
		console.log("Message sent: " + response.message);
	}

	// When finished with transport object do following....
	smtpTransport.close();
});
