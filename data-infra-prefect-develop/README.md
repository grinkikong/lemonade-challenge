# data-infra-prefect

work with staging prefect server:

`prefect config set PREFECT_API_URL="http://prefect-ui.stg.ludeo.com/api"`

`docker build -t data-infra-prefect:prefect-latest .`

`aws ecr get-login-password --region eu-central-1 --profile data-staging | docker login --username AWS --password-stdin 059419986680.dkr.ecr.eu-central-1.amazonaws.com`

`docker tag data-infra-prefect:prefect-latest 059419986680.dkr.ecr.eu-central-1.amazonaws.com/data-infra-prefect:prefect-latest`

`docker push 059419986680.dkr.ecr.eu-central-1.amazonaws.com/data-infra-prefect:prefect-latest`
