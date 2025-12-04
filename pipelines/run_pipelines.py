import papermill as pm
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

bronze_pipelines = [
    ("../scripts/bronze/customer.ipynb", f"../scripts/bronze/outputs/bronze_customer_{timestamp}.ipynb"),
    ("../scripts/bronze/product.ipynb", f"../scripts/bronze/outputs/bronze_product_{timestamp}.ipynb"),
    ("../scripts/bronze/order_header.ipynb", f"../scripts/bronze/outputs/bronze_order_header_{timestamp}.ipynb"),
    ("../scripts/bronze/order_details.ipynb", f"../scripts/bronze/outputs/bronze_order_details_{timestamp}.ipynb")
]

silver_pipelines = [
    ("../scripts/silver/dim_customer.ipynb", f"../scripts/silver/outputs/silver_dim_customer_{timestamp}.ipynb"),
    ("../scripts/silver/dim_product.ipynb", f"../scripts/silver/outputs/silver_dim_product_{timestamp}.ipynb"),
    ("../scripts/silver/dim_date.ipynb", f"../scripts/silver/outputs/silver_dim_date_{timestamp}.ipynb"),
    ("../scripts/silver/fact_order_line.ipynb", f"../scripts/silver/outputs/silver_fact_order_line{timestamp}.ipynb"),
    ("../scripts/silver/fact_order.ipynb", f"../scripts/silver/outputs/silver_fact_order{timestamp}.ipynb")
]

gold_pipelines = [
    ("../scripts/gold/dim_features.ipynb", f"../scripts/gold/outputs/gold_dim_features{timestamp}.ipynb")
]

print("=== Iniciando ejecución automática de notebooks ===")

for nb_in, nb_out in bronze_pipelines:
    print(f"[RUN] {nb_in}")
    pm.execute_notebook(nb_in, nb_out)
    print(f"[OK]  Output: {nb_out}")

for nb_in, nb_out in silver_pipelines:
    print(f"[RUN] {nb_in}")
    pm.execute_notebook(nb_in, nb_out)
    print(f"[OK]  Output: {nb_out}")

for nb_in, nb_out in gold_pipelines:
    print(f"[RUN] {nb_in}")
    pm.execute_notebook(nb_in, nb_out)
    print(f"[OK]  Output: {nb_out}")

print("=== Pipeline completado ===")