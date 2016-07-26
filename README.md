# elastic-monitor
Script is fill gape for elasticsearch marvel when marvel is not take enough parameters from system

# dependencies
   * psutils
   * pyyaml
   * logging
   * elasticsearch
   * apscheduler
   
#Delete manager 
##delete indexes by configuration 
   configuration is elasticsearch based. Stored josn format is
   ```javascript
        { 
            "template": ".marvel*",
            "ttl": 20,
            "units": "days",
            "cluster": {
                "host": "localhost",
                "port": 9200,
                "user": null,
                "password": null
            }
        }
   ```