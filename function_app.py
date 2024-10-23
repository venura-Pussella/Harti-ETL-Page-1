# import azure.functions as func
# import logging
# from main_manual import run_main

# app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# @app.route(route="harti1http")
# def harti1http(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Harti Page 1 ETL triggered.')

#     name = req.params.get('name')
#     if not name:
#         try:
#             req_body = req.get_json()
#         except ValueError:
#             pass
#         else:
#             name = req_body.get('name')

#     run_main()

#     # if name:
#     #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
#     # else:
#     #     return func.HttpResponse(
#     #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
#     #          status_code=200
#     #     )

    
#     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")

import azure.functions as func
import logging
from main import run_main

app = func.FunctionApp()

# @app.schedule(schedule="0 */10 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False) 
@app.schedule(schedule="0 30 10 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False) 
def harti_etl_page1(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Harti Page 1 ETL triggered.')
    run_main()
