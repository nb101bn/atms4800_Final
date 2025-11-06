function update_map(){
    var image_source = document.getElementById("map_frame").src;
    var image_source_query = image_source.split('?')[0];
    var d = new Date();
    var seconds = Math.round(d.getTime()/1000);
    image_source = image_source_query + '?t=' + seconds;
    document.getElementById("map_frame").src = image_source;
}

function changeImage(imgID, newImage, descId, altID) {
    var d = new Date();
    var seconds = Math.round(d.getTime()/1000);
    document.getElementById(imgID).src= newImage+'?t='+seconds;
    document.getElementById("map_description").innerHTML =desc_text[descId];
    document.getElementById(imgID).alt = alt_text[altID];
    var new_url = window.location.href.split('#')[0];
    new_url += '#'+imgID;
    window.location.href = new_url;
    return false;
}