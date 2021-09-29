var w = window.innerWidth - 50;
var h = window.innerHeight- 75;
var map = d3.select('#map')
			  .append('svg')
			  	.attr('height',h)
			  	.attr('width',w)
			  	.style('background','grey')
var local = d3.local();
var login_box = document.getElementById('login_box');

// Image =====================================================
let img;
function preload() {
  // preload() runs once
  img = loadImage('logo.jpg');
}
// ===========================================================


// Hover  =====================================================
let mouseOver = function(d) {
    local.set(this, d3.select(this).style("fill"));
     d3.select(this)
       .style("opacity", 0.5)
       .style("stroke", "black")  
    Tooltip.html(d.Count)
           .style("opacity",1)
    }

let mouseMove = function(event,d){
    Tooltip.html(d.properties.name + "<br>" + d.count)
    .style("left", event.pageX+25+"px")
    .style("top",  event.pageY-25+ "px")
}

let mouseLeave = function(d){
    d3.select(this)
    .style("stroke","white")
    .style("fill",local.get(this))
    .style("opacity",1)     
    Tooltip.html("")
    .style("opacity",0)
}

// create tooltip
var Tooltip = d3.select("#container")
                .append("div").attr("class", "tooltip")  
                .style("opacity", 0)
                .style("background", "lightgreen")
                .style("border", "solid")
                .style("border-width", "2px")
                .style("border-radius", "5px")
                .style("padding", "5px")
                .style("width","100px")
                .style("left", "1px")
                .style("top",  "1px")
                .style("position","absolute")
//  ===========================================================

// Map  =====================================================
var cScale = d3.scaleThreshold().domain([1,10,100,500,1000])
                                .range(["grey","lightblue","skyblue","royalblue","blue","darkblue"])
// load data  
var worldmap = d3.json("countries_new.geojson");

var svg = d3.select("div#container")
            .append("svg")
            .style("background-color","#c9e8fd")
            .attr("viewBox", [0,0,w,h])
            .attr("visibility","hidden")
            .classed("svg-content", true);


const projection = d3.geoAitoff()
                    .scale(d3.min([window.innerWidth /1.75/Math.PI,window.innerHeight/1.25/Math.PI]))
                    .translate([w / 2, h / 2]);

var path = d3.geoPath().projection(projection);

Promise.all([worldmap]).then(function(values){    
 // draw map
    svg.selectAll("path")
        .data(values[0].features)
        .enter()
        .append("path")
        .attr("fill", function(d){
            return cScale(+d.count);
        } )
        .attr("stroke","white")
        .attr("stroke-width","0.1px")
        .attr("class","continent")
        .attr("d", path)    
        .on("mouseover",mouseOver)
        .on("mousemove",mouseMove)
        .on("mouseleave",mouseLeave)
    svg.call(d3.zoom()
        .scaleExtent([1,20])
        .on("zoom",zoomed)
        )
    function zoomed({transform}) {
        svg.selectAll("path")
           .attr("transform", transform);
      }
    });
//  ================================================================

// Legend ================================================================
var legend = d3.legendColor()
    .labelFormat(d3.format(".0f"))
    .labels(d3.legendHelpers.thresholdLabels)
    .scale(cScale)

svg.append("g")
   .attr("class", "legend")
   .attr("transform", "translate(0,"+(h-150)+")")
   .attr("opacity",1);
console.log(h)
svg.select(".legend")
   .call(legend);

svg.append('svg:image')
    .attr('xlink:href', 'logo.jpg')
    .attr("width", w)
    .attr("height", h)
    .attr("x", 0)
    .attr("y", 0)
    .attr("opacity",0.02);
// =======================================================================

// Login==================================================================
function login()
    {
        var uname = document.getElementById("input_id").value;
        var pwd = document.getElementById("input_pw").value;
        var filter = "FTSHK";
        if(uname =='')
        {
            alert("please enter user name.");
        }
        else if(pwd=='')
        {
            alert("enter the password");
        }
        else if((uname!="user")||(pwd!="password"))
        {
            alert("Wrong username or password");
        }

        else
        {
            alert('Success');
            svg.attr("visibility","visible")
            login_box.style.display = "none"
        }
    }
//Reset Input
function clearFunc()
{
    document.getElementById("input_id").value="";
    document.getElementById("input_pw").value="";
}   
// =======================================================================

