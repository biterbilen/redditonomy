"use strict";

function build_taxonomy_table(results) {
    for (var i = 0; i < results.length; i++) {
        $('#taxonomy tbody')
            .append($('<tr>')
                .append($('<td>').text(results[i][i]['_1']))
                .append($('<td>').text(results[i][i]['_2']))
            );
    }
}

function initialize_slider() {
    $('input[type="range"]').rangeslider({
        polyfill: false,

        // Default CSS classes
        rangeClass: 'rangeslider',
        disabledClass: 'rangeslider--disabled',
        horizontalClass: 'rangeslider--horizontal',
        fillClass: 'rangeslider__fill',
        handleClass: 'rangeslider__handle',

        // Callback function
        onInit: function() {},

        // Callback function
        onSlide: function(position, value) {
            var $handle = this.$range.find('.rangeslider__handle__value');
            var results = $(document).data('results');
            var thisEntry = results[this.value];

            $('#date').text('week of ' + thisEntry['date']);
            $('#taxonomy tbody').empty();
            build_taxonomy_table(thisEntry['results']);

            $('#vocab').text('Vocab size: ' + thisEntry['vocab_size']);
            $('#documents').text('Number of documents: ' + thisEntry['num_docs']);
        },

        // Callback function
        onSlideEnd: function(position, value) {}
    });
}

$(document).ready(function() {
    initialize_slider();

    $('#subreddit-selector').on('change', function() {
        if (this.value) {
            var subreddit = this.value;

            $(document).data('subreddit', subreddit);

            var url = "http://127.0.0.1:5001/q/" + subreddit;
            console.log(url);
            $.ajax({
                url: url,
                dataType: 'json',
                async: false,
                success: function(data) {
                    $(document).data('results', data);
                }
            });

            var results = $(document).data('results');
            console.log(subreddit);
            console.log(results[0]);
            $('#subreddit').text('/r/' + subreddit);

            $('#date').text('week of ' + results[0]['date']);
            $('#taxonomy tbody').empty();
            build_taxonomy_table(results[0]['results']);

            $('#vocab').text('Vocab size: ' + results[0]['vocab_size']);
            $('#documents').text('Number of documents: ' + results[0]['num_docs']);

            var numweeks = results.length - 1;
            $('#rangeslider-wrapper').empty()
                .html('<input type="range" value="0" min="0" max="' + numweeks + '" step="1">');
            initialize_slider();
        } else {
            $('#subreddit').text();
            $('#date').text();
        }
    });
});
