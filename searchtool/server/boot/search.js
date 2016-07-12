exports.buildSearch = function (req, res) {
    var config   = require('../server-config.js');
    var moment   = require('moment');
    var unirest  = require('unirest');
    var humanize = require('humanize');
    var paginate = require('nodejs-yapaginate/lib/main.js');
    /*
       example Payload of sessionCookie
       { ttl: '1209600',
       id: '2opQLpAHH0ofjkM9TXjnsolxs5LLES00xHbn9sqKU5DSKgpGkBrMor5th7tYf8ep',
       userId: 1,
       user:
       { realm: null,
       username: null,
       credentials: null,
       challenges: null,
       email: 'darren.culbreath@uspto.gov',
       emailVerified: null,
       status: null,
       created: null,
       lastUpdated: null,
       id: 1 } }
       */
    
    // Setting Variables
    var q, fq;
    fq = "&fq=type:" + req.query.dataset;
    var dateRange='';

    // Get From Date
    // Only doing this for readiblity. Do not accept blank or undefined dates
    if ((typeof req.query.fromdate !== 'undefined' && req.query.todate !== 'undefined') &&  (req.query.fromdate.length > 2 && req.query.todate.length > 2)) {
        var fromDate = +new Date(req.query.fromdate)/1000;
        var toDate   = +new Date(req.query.todate)/1000;
        dateRange = 'doc_date:['+ fromDate+'%20TO%20'+toDate+']';
        fq += "&fq=" + dateRange;
    }

    // Ensure q var is cast to string
    if (req.query.q) {
        q=req.query.q.toString();
    }

    // Art Unit Filter
    if ((typeof req.query.art_unit !== 'undefined') && (req.query.art_unit.length < 7) && (req.query.art_unit.length > 0)) {
      var artUnit = 'dn_dw_dn_gau_cd:' + req.query.art_unit;
      fq += "&fq=" + artUnit;
    } 
    
    // Set Pagination to incremnt by 20 results
    var s = 0;
    var currentPage = 1;
    if (req.query.pageno && req.query.pageno > 0) {
        s = (req.query.pageno -1) *20;
        currentPage = parseInt(req.query.pageno) ;
    }

    // Set documentcode filter
    if ((typeof req.query.documentcode !== 'undefined') && (req.query.documentcode.length > 0)) {
            documentcode = 'documentcode:' + req.query.documentcode;
            fq += "&fq=" + documentcode;
    }

    // Build Search .. if no page number set then only show
    var SEARCH_URL = config.solrURI+'/select?q={!q.op=AND df=textdata}'+q+fq+'&wt=json&indent=true&rows=20&start='+s+'&hl=true&hl.snippets=10&hl.fl=textdata&hl.fragsize=200&hl.simple.pre=<code>&hl.simple.post=</code>&hl.usePhraseHighlighter=true&q.op=AND';
    if (req.query.dataset == 'oa'){
      //  SEARCH_URL += '&fl=appid,action_type,filename,minread,id,textdata';
    }else if (req.query.dataset == 'ptab'){
	    var ptab = true;
            //add query fields here later for ptab
   }

    // Debug for logs
    console.log(SEARCH_URL);

    var body;
    if (typeof req.query.q !== 'undefined') {
        unirest.get(SEARCH_URL).end(function (response) {
            if (typeof response.body !== 'undefined') {
                body = JSON.parse(response.body);
            }
            if (body && typeof body.response.docs !== 'undefined') {
                res.render('newview', {
                    result:body.response.docs,
                    total:humanize.numberFormat(body.response.numFound,0),
                    pagein:paginate({totalItem:body.response.numFound, itemPerPage:20, currentPage:currentPage, url:'/newsearch',params: {q: q, dataset: req.query.dataset, fromdate: req.query.fromdate, todate: req.query.todate, art_unit: req.query.art_unit, documentcode: req.query.documentcode} }),
                    took:humanize.numberFormat(body.responseHeader.QTime,0 ),
                    highlighting:body.highlighting,
                    term:q,
                    ptab: ptab,
                    email: req.body.email,
                    accessOK: !!(! config.requireLogin || token.id),
                    todate: req.query.todate,
                    fromdate:req.query.fromdate,
                    documentcode: req.query.documentcode,
                    artUnit: req.query.art_unit
                });
            } else {
                res.render('newview', {
                    total:0,
                    pagein:'',
                    took:body.responseHeader.QTime,
                    term:q,
                    email: req.body.email,
                    accessOK: !!(! config.requireLogin || token.id)
                });
            }
        });
    } else {
        res.render('newview', {
            email: req.body.email,
            accessOK: !!(! config.requireLogin || token.id)
        });
    }
};
