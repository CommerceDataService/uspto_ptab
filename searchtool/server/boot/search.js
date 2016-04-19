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
    // Get From Date
    var dateRange='';
    // Only doing this for readiblity. Do not accept blank or undefined dates

    if ((typeof req.query.fromdate !== 'undefined' && req.query.todate !== 'undefined') &&  (req.query.fromdate.length > 2 && req.query.todate.length > 2)) {
        var fromdate =  moment(req.query.fromdate).format('YYYY-MM-DD');
        var todate   =  moment(req.query.todate).format('YYYY-MM-DD');

        todate = todate + 'T00:00:00Z';
        fromdate = fromdate + 'T00:00:00Z';

        dateRange = '%20AND%20doc_date:['+ fromdate+'%20TO%20'+todate+']';
    }

    // Ensure q var is cast to string
    var q;
    if (req.query.q) {
        q=req.query.q.toString();
    }

    // Set Pagination to incremnt by 20 results
    var s = 0;
    var currentPage = 1;
    if (req.query.pageno && req.query.pageno > 0) {
        s = (req.query.pageno -1) *20;
        currentPage = parseInt(req.query.pageno) ;
    }

    q = q+dateRange;

    // Build Search .. if no page number set then only show
    var SEARCH_URL = config.solrURI+'/oafiledatanew/select?q='+q+'&wt=json&indent=true&rows=20&start='+s+'&hl=true&hl.snippets=10&hl.fl=textdata&hl.fragsize=200&hl.simple.pre=<code>&hl.simple.post=</code>&hl.usePhraseHighlighter=true&q.op=AND&fl=appid,action_type,filename,minread,id,textdata';

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
                    pagein:paginate({totalItem:body.response.numFound, itemPerPage:20, currentPage:currentPage, url:'/newsearch',params:{q:q}}),
                    took:humanize.numberFormat(body.responseHeader.QTime,0 ),
                    highlighting:body.highlighting,
                    term:q
                });
            } else {
                res.render('newview', {
                    total:0,
                    pagein:'',
                    took:body.responseHeader.QTime,
                    term:q
                });
            }
        });
    } else {
        res.render('newview');
    }
};
