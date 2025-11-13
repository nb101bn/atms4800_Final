document.onreadystatechange = function() {
    if (document.readyState === 'complete') {
        var url = window.location.href;
        var queryString = url ? url.split('#')[1] : window.location.search.slice(1);
        for (var key in map_address) {
            if(queryString == key){
                default_flag = 0;
                document.getElementById("map_frame").src=map_address[key];
                document.getElementById("map_description").innerHTML=desc_text[key];
            }
        }
        if(default_flag){
            document.getElementById("map_frame").src="images/maps/full/interpolated_air_temp.png";
            document.getElementById("map_description").innerHTML=desc_text["air_temp"];
        }
    }
};
function update_map(){
    var image_source = document.getElementById("map_frame").src;
    var image_source_query = image_source.split('?')[0];
    var d = new Date();
    var seconds = Math.round(d.getTime()/1000);
    image_source = image_source_query + '?t=' + seconds;
    document.getElementById("map_frame").src = image_source;
};

function changeImage(imgID, newImage, descId, altID) {
    var d = new Date();
    var seconds = Math.round(d.getTime()/1000);
    document.getElementById(imgID).src= newImage+'?t='+seconds;
    document.getElementById("map_description").innerHTML =desc_text[descId];
    document.getElementById(imgID).alt = altID;
    var new_url = window.location.href.split('#')[0];
    new_url += '#'+descId;
    window.location.href = new_url;
    return false;
};