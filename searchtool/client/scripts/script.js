var windowWidth = $(window).width();

// resize window only if width changes
$(window).resize(function() {
    if ($(window).width() !== windowWidth) {

        windowWidth = $(window).width();

        $('.app-sidebar').removeClass('open');
        $('.btn-pgActions, .btn-displayOpts, .pager').removeClass('hide');
    }
});


// toggle Collections aside display
$('.btn-collections')
    .removeClass('hide')
    .click(function() {
        $('.collections').toggleClass('hide');
        $('#chk-collections').prop('checked', $('.collections').is(':visible'));
    });

// events for Collections panel
$('.panel--collections')
    .on('click', '.link-appNumSearch', function(e){
        var appNum = $(this).find('.appNum').text();
        console.log(appNum);
        e.preventDefault();

        $('#txt-search, #txt-find').val(appNum);
    })
    .on('click', '.close', function(e){
        e.preventDefault();

        $('.collections').addClass('hide');
         $('#chk-collections').prop('checked', false);
    })
    .on('click', '[data-toggle="collapsible"]', function(e){
        var trigger = $(this),
            target = trigger.attr('href'),
            expanded = trigger.attr('aria-expanded') === 'true';

        e.preventDefault();
        trigger.attr('aria-expanded', !expanded).find('.icon').toggleClass('icon-plus-square-o icon-minus-square-o');
        $(target).find('tr').toggleClass('hide').attr('aria-hidden', expanded);
    });

// initialize tooltip for table cols toggle
$('.btn-tblCols').tooltip({
    placement: 'top',
    template: '<div class="tooltip tooltip-light" role="tooltip"><div class="tooltip-arrow"></div><div class="tooltip-inner"></div></div>'
});

// enable datepickers
$('.datepicker').datepicker({
    // autoSize: true,
    // showButtonPanel: true,
    numberOfMonths: 1,
    showOtherMonths: true
    // changeMonth: true,
    // changeYear: true
});

// toggle filters sidebar
$('.link-sidebar').click(function(e) {
    e.preventDefault();
    $('.app-sidebar').toggleClass('open');
    $('.btn-pgActions, .btn-displayOpts, .pager').toggleClass('hide');
});

// hide filters sidebar
$('.btn-closeSidebar').click(function(e) {
    e.preventDefault();
    $('.app-sidebar').removeClass('open');
    $('.btn-pgActions, .btn-displayOpts, .pager').removeClass('hide');
});

// toggle UI configurator
$('.config-ui')
    .on('click', '.toggle', function() {
        $('.config-ui').toggleClass('open');
    });

// toggle switches for UI widgets
$('.switch')
    .on('click', 'input[type="checkbox"]:not(.js-chkPanels)', function() {
        var attr = '.' + $(this).attr('data-target');

        $(attr).toggleClass('hide');
    })
     .on('click', '#chk-leftPane, #chk-rightPane', function() {
        var attr = '.' + $(this).attr('data-target');

        $(attr).toggleClass('open');
    })
    .on('click', 'input[name="tblsSwitch"]', function() {
        var attr = '.' + $(this).attr('data-target');

        $('.js-tableSwitch').addClass('hide');
        $(attr).toggleClass('hide');
    })
    .on('click', 'input[name="filtersSwitch"]', function() {
        var attr = '.' + $(this).attr('data-target');

        $('.js-filterSwitch').addClass('hide');
        $(attr).toggleClass('hide');
    });

var documentcodeObj = [
    'RXCTIN',
    'RDIN',
    'CTNF',
    'CNTA',
    'CTFR',
    'CNOA',
    'N271',
    'REXD',
    'EX.R',
    'RTDE',
    'INTNO',
    'L.SI',
    'RXBPAI',
    'RXSRXI',
    'CTMS',
    'M327',
    'M327-E',
    'M327-D',
    'P132',
    'R327',
    'RXM327',
    'RXMISC',
    'RXMMIS',
    'SEMI',
    'RTGR',
    'RTDI'
];

var URL = window.location.href;
documentcodeObj.forEach(function(value,index) {
    if (URL.indexOf(value) > -1) {
        $('#documentcodeID option[value="' + value + '"]').prop('selected', true);
    }
})