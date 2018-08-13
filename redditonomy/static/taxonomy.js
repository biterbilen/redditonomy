"use strict";

function build_taxonomy_table(results, name) {
    $('<div>').addClass(
        'mui-col-sm-4 mui-col-md-4 mui-col-lg-4'
    ).append(
        $('<div>').addClass(
            'mui-panel'
        ).css({
            'padding': '50px'
        }).append(
            $('<div>').addClass(
                'mui-row'
            ).append(
                $('<div>').addClass(
                    'mui-col-sm-12'
                ).append(
                    $('h2').text(name)
                )
            )
        ).append(
            $('<table>').addClass(
                'mui-table mui-table--bordered'
            ).attr(
                'id', 'taxonomy'
            ).append(
                $('<thead>').append(
                    $('<tr>').append(
                        $('<th>').text('term')
                    ).append(
                        $('<th>').text('score')
                    )
                )
            ).append(
                $('<tbody>')
            )
        )
    ).appendTo('#results');

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
        onInit: function() {},

        // Callback function
        onSlideEnd: function(position, value) {}
    });
}

function build_chart(results) {
    $('<div>').addClass(
        'mui-col-sm-8'
    ).append(
        $('<div>').addClass(
            'mui-panel'
        ).css(
            {'padding': '50px'}
        ).append(
            $('<canvas>').attr({
                id: 'chart',
                width: 600,
                height: 400
            })
        )
    ).prependTo('#results');

    var weeks = $.map(results, function(element) {
        return element['date'];
    });
    var vocabSizes = $.map(results, function(element) {
        return element['vocab_size'];
    });
    var corpusSizes = $.map(results, function(element) {
        return element['num_docs'];
    });
    var blue = 'rgba(33, 150, 243,0.2)';
    var green = 'rgba(99, 210, 151, 0.2)';
    var newChart = new Chart($('#chart'), {
        type: 'line',
        data: {
            labels: weeks,
            datasets: [
                {
                    label: 'Corpus Size',
                    data: corpusSizes,
                    backgroundColor: [
                        blue,
                        green,
                    ],
                    borderColor: [
                        blue,
                        green,
                    ],
                    borderWidth: 1
                },
                {
                    label: 'Vocab Size',
                    data: vocabSizes,
                    backgroundColor: [
                        green,
                        blue,
                    ],
                    borderColor: [
                        blue,
                        green,
                    ],
                    borderWidth: 1
                }
            ]
        },
        options: {
            scales: {
                yAxes: [{
                    ticks: {
                        beginAtZero: true
                    }
                }]
            }
        }
    });
}

$(document).ready(function() {
    initialize_slider();

    $('#subreddit-selector').on('change', function() {
        if (this.value) {
            var subreddit = this.value;

            $(document).data('subreddit', subreddit);

            var url = "http://127.0.0.1:5001/q/" + subreddit;
            $.ajax({
                url: url,
                dataType: 'json',
                async: false,
                success: function(data) {
                    $(document).data('results', data);
                }
            });

            var results = $(document).data('results');
            $('#subreddit').text('/r/' + subreddit);

            $('#date').text('week of ' + results[0]['date']);
            $('#taxonomy tbody').empty();
            build_taxonomy_table(results[0]['results'], 'online');

            $('#vocab').text('Vocab size: ' + results[0]['vocab_size']);
            $('#documents').text('Number of documents: ' + results[0]['num_docs']);

            var numweeks = results.length - 1;
            $('#rangeslider-wrapper').empty()
                .html('<input type="range" value="0" min="0" max="' + numweeks + '" step="1">');
            initialize_slider();
            build_chart(results);
        } else {
            $('#subreddit').text();
            $('#date').text();
        }
    });
});
