"use strict";

function build_taxonomy_table(name) {
    $('#results').html(
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
                    ).append(
                        $('h3').addClass(
                            'mui--text-accent-secondary'
                        ).text(name)
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
        )
    );
}

function fill_taxonomy_table(results) {
    for (var i = 0; i < results.length; i++) {
        $('#taxonomy tbody')
            .append($('<tr>')
                .append($('<td>').text(results[i][i]['_1']))
                .append($('<td>').text(results[i][i]['_2']))
            );
    }
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
            build_taxonomy_table('online');
            fill_taxonomy_table(results[0]['results']);

            $('#vocab').text('Vocab size: ' + results[0]['vocab_size']);
            $('#documents').text('Number of documents: ' + results[0]['num_docs']);

            var numweeks = results.length - 1;
            $('#rangeslider-wrapper').empty()
                .html('<input type="range" value="0" min="0" max="' + numweeks + '" step="1">');
            build_chart(results);
        } else {
            $('#subreddit').text();
            $('#date').text();
        }
    });
});
