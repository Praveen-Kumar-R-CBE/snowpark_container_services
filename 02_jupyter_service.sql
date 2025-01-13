USE ROLE CONTAINER_USER_ROLE;
USE DATABASE CONTAINER_HOL_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE CONTAINER_HOL_WH;
create service CONTAINER_HOL_DB.PUBLIC.JUPYTER_SNOWPARK_SERVICE
in compute pool CONTAINER_HOL_POOL
from @specs
specification_file='jupyter-snowpark.yaml'
external_access_integrations = (ALLOW_ALL_EAI);

CALL SYSTEM$GET_SERVICE_STATUS('CONTAINER_HOL_DB.PUBLIC.jupyter_snowpark_service');
CALL SYSTEM$GET_SERVICE_LOGS('CONTAINER_HOL_DB.PUBLIC.JUPYTER_SNOWPARK_SERVICE', '0', 'jupyter-snowpark',10);

SHOW ENDPOINTS IN SERVICE JUPYTER_SNOWPARK_SERVICE;

--- After we make a change to our Jupyter notebook, we will suspend and resume the service and you can see that the changes we made in our Notebook are still there!
ALTER SERVICE CONTAINER_HOL_DB.PUBLIC.JUPYTER_SNOWPARK_SERVICE SUSPEND;
ALTER SERVICE CONTAINER_HOL_DB.PUBLIC.JUPYTER_SNOWPARK_SERVICE RESUME;