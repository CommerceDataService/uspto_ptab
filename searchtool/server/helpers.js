var hbs = require('hbs');
hbs.registerHelper('truncate', function(passedString) {
    var theString = passedString.substring(0,900);
    return new hbs.SafeString(theString)
});
hbs.registerHelper('breaklines', function(text) {
    text = hbs.Utils.escapeExpression(text);
    text = text.replace(/(\r\n|\n|\r)/gm, '<br>');
    text = text.replace(/[&\/\\]/g,'_');
    return new hbs.SafeString(text);
});
hbs.registerHelper('convertDocCode', function(code){
  var documentCodeDictionary ={
    'M327-D': 'PUB OTHER MISCELLANEOUS COMMUNICATION TO APPLICANT',
    'CTNF': 'NON-FINAL REJECTION',
    'CNTA': 'ALLOWABILITY NOTICE',
    'CTFR': 'FINAL REJECTION',
    'CNOA': 'CORRECTED NOTICE OF ALLOWABILITY',
    'N271': 'RESPONSE TO AMENDMENT UNDER RULE 312',
    'REXD': 'RX - EX PARTE REEXAM ORDER - DENIED',
    'EX.R': 'REASONS FOR ALLOWANCE',
    'RTDE': 'RX - PETITION DECISION - DENIED',
    'INTNO': 'INTERFERENCE NOTIFICATION TO PATENTEE RULE 607',
    'L.SI': 'SUSPENSION - INTERFERENCE IN ANOTHER CASE',
    'RXBPAI': 'REEXAM FORWARDED TO BOARD OF PATENT APPEALS AND INTERFERENCES',
    'RXSRXI': 'DECISION STAYING REEXAM PROCEEDING IN FAVOR OF INTERFERENCE PROCEEDING',
    'CTMS': 'MISCELLANEOUS ACTION WITH SSP',
    'M327': 'MISCELLANEOUS COMMUNICATION TO APPLICANT - NO ACTION COUNT',
    'M327-E': 'BOA MISCELLANEOUS COMMUNICATION TO APPLICANT',
    'P132': 'MISCELLANEOUS COMMUNICATION',
    'R327': 'RX - MISCELLANEOUS COMMUNICATION TO APPLICANT',
    'RXM327': 'MISCELLANEOUS COMMUNICATION TO APPLICANT',
    'RXMISC': 'MISCELLANEOUS ACTION MAILED',
    'RXMMIS': 'MISCELLANEOUS LETTER MAILED',
    'SEMI': 'SE - MISCELLANEOUS DECISION/OFFICE NOTICE IN SE',
    'RTGR': 'RX - PETITION DECISION - GRANTED',
    'RTDI': 'RX - PETITION DECISION - DISMISSED'
  };
  return new hbs.SafeString(documentCodeDictionary[code]);
});

hbs.registerHelper('hyperlinkIt', function(source, appid){
  switch(source){
    case 'palm': return new hbs.SafeString('http://palmapps.uspto.gov/cgi-bin/expo/GenInfo/snquery.pl?APPL_ID='+appid);
    case 'dav': return new hbs.SafeString('http://dav.uspto.gov/webapp/applicationViewer.html?casenumber='+appid)
  }
});

module.exports = hbs;
