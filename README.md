# dataproc
gsutil cp dataproc-etl-1.0.jar  gs://dataproc-jar-folder/


gcloud dataproc jobs submit spark ^
    --cluster=my-test-cluster ^
    --region=us-central1 ^
    --class=demo.DataProcML ^
    --jars=gs://dataproc-jar-folder/dataproc-etl-1.0.jar 
