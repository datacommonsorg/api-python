**[Temporary] Schema/Datacommons Deployment instructions**

These assume that the version of SDOAPP in use is in schemaorg branch ‘**vocabindi2**’ , and the data files for datacommons/schema are in repository ‘**https://github.com/RichardWallis/datacommons/tree/appupdate**’.

1.  Checkout SDOAPP - branch vocabindi2  
    git clone https://github.com/schemaorg/schemaorg.git  
    git checkout vocabindi2  

2.  Get Datacommons deployment files  
    curl -O https://raw.githubusercontent.com/RichardWallis/datacommons/appupdate/schema/datacomschema.yaml  
    curl -O https://raw.githubusercontent.com/RichardWallis/datacommons/appupdate/schema/test-datacomschema.yaml  

3.  Deploy to an app engine:  
    Note: change project name from sdo-rjwtest to appropriate one.  
    Note Change —version=2 to one that is not used in target project  

    If using files in Richard/Wallis/datacommons repo branch appupdate:  
    scripts/appdeploy.sh --no-promote --project sdo-rjwtest --version=2 test-datacomschema.yaml  

    When appupdate branch has been merged in to datacommons:  
    scripts/appdeploy.sh --no-promote --project sdo-rjwtest --version=2 datacomschema.yaml  

4.  Migrate traffic to version 2 (or whichever version label is chosen)  

5.  To see which configuration files are in use:  
    * In console/App Engine/ Versions select Config>View for the version  
    * Scroll to env_variables  
    * Copy value of CONFIGFILE  
    * Paste into browser to view  
    * DATACOMMLOC value is root location for Datacommons config files  
    * SCHEMAORGLOC value is root location of Schema.org config files referenced in displays