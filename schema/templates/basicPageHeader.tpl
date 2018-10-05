<!-- Header start from basicPageHeader.tpl -->

<!-- Google Tag Manager (noscript) -->
<noscript>
  <iframe src="https://www.googletagmanager.com/ns.html?id=GTM-596VBP2"
          height="0" width="0" style="display: none; visibility: hidden"></iframe>
</noscript>
<!-- End Google Tag Manager (noscript) -->

<div id="container">
	<div id="intro">
		<div id="pageHeader">
			<div class="wrapper">
				<div id="sitename">
				<h1>
					<a href="/">{{ sitename }}</a>
				</h1>
				</div>
				<!-- <div id="cse-search-form" style="width: 400px;"><div class="gcse-searchbox-only" data-resultsUrl="{{ docsdir }}search_results.html"></div></div> -->
			</div>
		</div>
	</div>
</div>
<div id="selectionbar">
	<div class="wrapper">
		<ul>
	        {% if menu_sel == "Documentation" %}
	        <li class="activelink">
	        {% else %}
	        <li>
	        {% endif %}
				<a href="https://datacommons.org/">Home</a>
			</li>
	        {% if menu_sel == "Schemas" %}
	        <li class="activelink">
	        {% else %}
	        <li>
	        {% endif %}
				<a href="https://schema-datacommons.appspot.com/">Schemas</a>
			</li>
			<li>
	        {% if home_page == "True" %}
				<a href="https://github.com/google/datacommons">Github</a>
	        {% else %}
				<a href="https://github.com/google/datacommons">Github</a>
	        {% endif %}
			</li>
		</ul>
	</div>
</div>

{% include 'topnotes.tpl' with context %}

<!-- Header end from basicPageHeader.tpl -->
