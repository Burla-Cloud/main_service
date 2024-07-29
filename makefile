
.ONESHELL:
.SILENT:


test:
	poetry run pytest -s --disable-warnings

service:
	poetry run uvicorn main_service:application --workers 6 --host 0.0.0.0 --port 5001 --reload

restart_test_cluster:
	AUTH_HEADER="Authorization:Bearer $${MAIN_SERVICE_API_KEY}"; \
	curl -X POST -H "$${AUTH_HEADER}" http://127.0.0.1:5001/restart_cluster

test_node:
	poetry run python -c "from main_service.node import Node; Node.start('n1-standard-96')"

deploy-test:
	set -e; \
	IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=burla-main-service \
			--location=us \
			--repository=burla-main-service \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	IMAGE_NAME=$$( echo \
		us-docker.pkg.dev/burla-test/burla-main-service/burla-main-service:$${IMAGE_TAG} \
	); \
	gcloud run deploy burla-main-service \
	--image=$${IMAGE_NAME} \
	--project burla-test \
	--region=us-central1 \
	--set-env-vars IN_PRODUCTION=False \
	--min-instances 0 \
	--max-instances 10 \
	--memory 4Gi \
	--cpu 1 \
	--timeout 3600 \
	--concurrency 10 \
	--allow-unauthenticated

deploy-test-image-to-prod:
	set -e; \
	IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=burla-main-service \
			--location=us \
			--repository=burla-main-service \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	IMAGE_NAME=$$( echo \
		us-docker.pkg.dev/burla-test/burla-main-service/burla-main-service:$${IMAGE_TAG} \
	); \
	gcloud run deploy burla-main-service-0-1-0 \
	--image=$${IMAGE_NAME} \
	--project burla-prod \
	--region=us-central1 \
	--min-instances 1 \
	--max-instances 20 \
	--memory 4Gi \
	--cpu 1 \
	--timeout 3600 \
	--concurrency 10 \
	--allow-unauthenticated

image:
	set -e; \
	IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=burla-main-service \
			--location=us \
			--repository=burla-main-service \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	NEW_IMAGE_TAG=$$(($${IMAGE_TAG} + 1)); \
	IMAGE_NAME=$$( echo \
		us-docker.pkg.dev/burla-test/burla-main-service/burla-main-service:$${NEW_IMAGE_TAG} \
	); \
	gcloud builds submit --tag $${IMAGE_NAME}; \
	echo "Successfully built Docker Image:"; \
	echo "$${IMAGE_NAME}"; \
	echo "";

container:
	set -e; \
	IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=burla-main-service \
			--location=us \
			--repository=burla-main-service \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	IMAGE_NAME=$$( echo \
		us-docker.pkg.dev/burla-test/burla-main-service/burla-main-service:$${IMAGE_TAG} \
	); \
	docker run --rm -it \
		--name main_service \
		-v $(PWD):/home/pkg_dev/app \
		-v ~/.gitconfig:/home/pkg_dev/.gitconfig \
		-v ~/.ssh/id_rsa:/home/pkg_dev/.ssh/id_rsa \
		-v ~/.config/gcloud:/home/pkg_dev/.config/gcloud \
		-e GOOGLE_CLOUD_PROJECT=burla-test \
		-e IN_DEV=True \
		-e IN_PRODUCTION=False \
		-p 5001:5001 \
		--entrypoint poetry $${IMAGE_NAME} run bash
