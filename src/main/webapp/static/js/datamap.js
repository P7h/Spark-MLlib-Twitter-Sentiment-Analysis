var source = new EventSource('/stream');

var worldmap = new Datamap({
    scope: 'world',
    title: 'Sentiment',
    projection: 'equirectangular',
    element: document.getElementById("worldmap"),
    geographyConfig: {
        popupOnHover: false,
        highlightOnHover: false
    },
    bubblesConfig: {
        radius: 7,
        exitDelay: 30000 // Milliseconds
    },
    responsive: true,
    done: function(datamap) {
        datamap.svg.call(d3.behavior.zoom().on("zoom", redraw));
        //$("#resetZoom").on("click", function(){ resetZoom(); })
        function redraw() {
            datamap.svg.selectAll("g").attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
        }

        function resetZoom() {
            datamap.svg.selectAll("g").attr("transform", "translate(0,0)scale(1.0)");
        }
    },
    fills: {
        defaultFill: '#E5DBD2',
        "0": 'blue',
        "-1": 'red',
        "1": 'green'
    }
});

worldmap.legend({
    //legendTitle: "Sentiment",
    labels: {
        "1": 'Positive',
        "0": 'Neutral',
        "-1": 'Negative'
    }
});

d3.select(window).on('resize', function() {
    worldmap.resize();
});

function determineColor(sentiment) {
    var newColor = sentiment == 0 ? "blue" : (sentiment == -1 ? "red" : "green");
    return newColor;
}

function determineEmoji(sentiment) {
    var newColor = sentiment == 0 ? "&#x1F44C;" : (sentiment == -1 ? "&#x1F44E;" : "&#128077;");
    return newColor;
}

var func = function(geo, data) {
    var url = "https://twitter.com/" + data.name + "/status/" + data.id;
    var tip = "<div><h3><span style='vertical-align:middle'>@" + data.name + '</span><img style="vertical-align:middle" height="70" width="70" src="' + data.pic + '"></h3></div>';
    tip += "<h6>" + data.date + "</h6>";
    tip += "<h4>" + data.text + "</h4>";
    tip += "Spark MLlib:<font size='6em' color=" + determineColor(parseInt(data.fillKey)) + ">" + determineEmoji(parseInt(data.fillKey)) + "</font>";
    tip += "<br>Stanford CoreNLP:<font size='6em' color=" + determineColor(parseInt(data.fillKey1)) + ">" + determineEmoji(parseInt(data.fillKey1)) + "</font>";
    return "<div class='hoverinfo tooltip'>" + tip + '</div>';
}

source.onmessage = function(event) {

    //console.log(event.data);
    if (event.data !== "1") {
        data = event.data.split("¦");
        var bubble = {
            "id": data[0],
            "name": data[1],
            "text": data[2],
            "fillKey1": data[3],
            "fillKey": data[4],
            "latitude": data[5],
            "longitude": data[6],
            "pic": data[7],
            "date": data[8]
        };

        var bubble_array = [];
        bubble_array.push(bubble);
        worldmap.bubbles(bubble_array, {
            popupTemplate: func
        });
    }

    //Added these placeholders for future reference
    /*d3.selectAll(".datamaps-bubble").on('click', function(bubble) {
        console.log(bubble);
    });

    d3.selectAll('#worldmap').on('mouseout', function(info) {
      if (d3.event.target.tagName == "circle"){
      	console.log(d3.select(d3.event.target).data()[0],"out")
      }
    });
    d3.selectAll('#worldmap').on('mouseover', function(info) {
      if (d3.event.target.tagName == "circle"){
      	console.log(d3.select(d3.event.target).data()[0],"over")
      }
    });*/

};