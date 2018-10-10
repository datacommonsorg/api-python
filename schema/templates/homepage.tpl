<!DOCTYPE html>
<html lang="en">
<head>
  {% include 'headtags.tpl' with context %}
    <title>Home - {{ sitename }}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="description" content="Schema.org is a set of extensible schemas that enables webmasters to embed
    structured data on their web pages for use by search engines and other applications." />
    <link rel="stylesheet" type="text/css" href="/docs/schemaorg.css">
</head>
<body>
{% include 'basicPageHeader.tpl' with context %}

<div id="mainContent">

<h1>Welcome to {{ sitename }}</h1>

<p>
This is the schema site for <a href="https://datacommons.org/">DataCommons.org</a>.
DataCommons uses <a href="https://schema.org/">schema.org</a> vocabulary. This site contains the additional schemas used by datacommons. At this stage
many of the terms still being refined and are not proposed for wider adoption. Comments are welcomed <a href="https://github.com/google/datacommons/issues">via Github</a>.
</p>

<p>The <a href="/docs/full.html">full view</a> listing shows all defined terms, or you
can jump directly to a commonly used term. For example, <a href="{{staticPath}}/StatisticalPopulation">StatisticalPopulation</a>,
<a href="{{staticPath}}/Measurement">Measurement</a>,
<a href="{{staticPath}}/NOAA_WeatherConditionEnum">NOAA_WeatherConditionEnum</a>,
<a href="{{staticPath}}/USC_CitizenStatusEnum">USC_CitizenStatusEnum</a>,
<a href="{{staticPath}}/DepthValue">DepthValue</a>, ...
</p>

<br/>
</div>



<div id="footer">
  <p> <a href="http://datacommons.org/faq">FAQ and Terms</a></p>
</div>



<p><br/></p>

</body>
</html>
