# projects/etl_base_project/src/utilities/worker 
from typing import Dict, Any
from projects.etl_base_project.src.common.logger import logger 
from projects.etl_base_project.src.utilities.etl_manager import ETLManager 
import time
import os

def wait_for_debug():
    if os.getenv("ENABLE_DEBUG") == "1":
        import debugpy
        if not debugpy.is_client_connected():
            debugpy.listen(("0.0.0.0", 5678))
            print("🔎 Waiting for VS Code debugger on :5678 ...")
            debugpy.wait_for_client()
            debugpy.breakpoint()


def run_partition_worker(source_db_conf, target_db_conf, **kwargs):
    """
    plan_partitions → .expand(op_kwargs=...) ile gelen payload:
      {
        "task_conf": {...},          # orijinal task config (bindings DAHİL olabilir)
        "where": "...",              # plan aşamasında RESOLVED (literal) where
        "order_by": "...",
        "label": "..."
      }
    """
    

    task_conf: Dict[str, Any] = dict(kwargs.get("task_conf", {}))
    where     = kwargs.get("where")
    order_by  = kwargs.get("order_by", "")
    label     = kwargs.get("label", "")
    sql_text  = kwargs.get("sql_text","")

    # Partition bazında TRUNCATE engeli (merkezi prepare zaten yapıldı/yapılacak)
    lm = (task_conf.get("load_method") or "").strip()
    if lm == "create_if_not_exists_or_truncate":
        task_conf["load_method"] = "create_if_not_exists"

    # Plan tarafında çözülen WHERE/ORDER BY'ı task_conf içine yedir
    if where:
        task_conf["where"] = where
    if order_by:
        task_conf["order_by"] = order_by
    if sql_text:
        task_conf["sql_text"] = sql_text.strip()

    #wait_for_debug() 

    # IMPORTANT: runtime'a bindings taşımayalım (resolved geldi)
    if "bindings" in task_conf:
        task_conf.pop("bindings", None)
        logger.info("Remover bindings: task_conf.pop")

    logger.info("[worker] effective load_method=%s", task_conf.get("load_method"))
    logger.info("[worker] effective_where=(%s) ORDER_BY=(%s)", task_conf.get("where"), task_conf.get("order_by",""))
    logger.info("[worker] [partition] %s WHERE=(%s) ORDER_BY=(%s)", label, task_conf.get("where"), task_conf.get("order_by",""))
    logger.info("[worker] [sql_text]= %s", (sql_text or "").strip())
    start = time.time()
    try:
        ETLManager(source_db_conf, target_db_conf).run_etl_task(**task_conf)
        duration = time.time() - start
        logger.info(f"[METRICS] partition={label} duration={duration:.2f}s status=SUCCESS")
    except Exception as e:
        duration = time.time() - start
        logger.error(f"[METRICS] partition={label} duration={duration:.2f}s status=FAILED error={e}")
        raise
 