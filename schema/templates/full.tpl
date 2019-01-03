<!DOCTYPE html>
<html lang="en">
<head>
  {% include 'headtags.tpl' with context %}
    <title>Full Hierarchy - {{ sitename }}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="description" content="Schema.org is a set of extensible schemas that enables webmasters to embed
    structured data on their web pages for use by search engines and other applications." />
    <script type="text/javascript" src="//ajax.googleapis.com/ajax/libs/jquery/1.5.1/jquery.min.js"></script>
    <link rel="stylesheet" type="text/css" href="/docs/schemaorg.css" />

<script type="text/javascript">
$(document).ready(function(){
    $('input[type="radio"]').click(function(){
        if($(this).attr("value")=="local"){
            $("#full_thing_tree").hide();
            $("#ext_thing_tree").hide();
            $("#thing_tree").show(500);
        }
        if($(this).attr("value")=="full"){
            $("#thing_tree").hide();
            $("#ext_thing_tree").hide();
            $("#full_thing_tree").show(500);
        }
        if($(this).attr("value")=="ext"){
            $("#thing_tree").hide();
            $("#full_thing_tree").hide();
            $("#ext_thing_tree").show(500);
        }
     });
     $("#full_thing_tree").hide();
	$("#ext_thing_tree").hide();
	$("#thing_tree").show();
});
</script>
</head>
<body style="text-align: center;">

{% include 'docsBasicPageHeader.tpl' with context %}

<div style="text-align: left; margin-left: 8%; margin-right: 8%">

<h3>DataCommons.org vocabulary</h3>

<p>TThis is the main vocabulary hierarchy: a collection of types (or "classes"), each of which has one or more parent types.
  Although a type may have more than one super-type, here we show each type in one branch of the tree only.
</p>
<p>
    The DataCommons site does not reproduce all the definitions from <a href="http://schema.org/">Schema.org</a>; see the
    corresponding <a href="https://schema.org/docs/full.html">Full listing</a> page for the larger vocabulary.
</p>

<br/>

<div>Vocabulary view:<br/>
    <div id="thing_tree">
    {{ full_thing_tree | safe }}
    </div>

    <div id="datatype_tree">
    {{ datatype_tree | safe }}
    </div>
</div>



<br/><br/>

</div>
</body>
</html>
