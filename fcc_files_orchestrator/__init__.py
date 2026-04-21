# fcc_files_orchestrator/__init__.py

import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    """
    Orchestrator: fans out to one activity per service type.
    """
    payload = context.get_input() or {}
    service_types = payload.get("service_types") or ["tv", "fm", "am", "cable"]

    tasks = []
    for st in service_types:
        tasks.append(context.call_activity("fcc_files_run", {
            "service_type": st,
            "payload": payload
        }))

    results = yield context.task_all(tasks)
    return {
        "status": "Completed",
        "service_types": service_types,
        "results": results
    }

main = df.Orchestrator.create(orchestrator_function)
