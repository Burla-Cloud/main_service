.ONESHELL:
.SILENT:

WEBSERVICE_NAME = burla-main-service
PYTHON_MODULE_NAME = main_service

ARTIFACT_REPO_NAME := $(WEBSERVICE_NAME)
ARTIFACT_PKG_NAME := $(WEBSERVICE_NAME)
TEST_IMAGE_BASE_NAME := us-docker.pkg.dev/burla-test-joe/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_PKG_NAME)
PROD_IMAGE_BASE_NAME := us-docker.pkg.dev/burla-prod/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_PKG_NAME)

test:
	poetry run pytest -s --disable-warnings

service:
	poetry run uvicorn $(PYTHON_MODULE_NAME):application --host 0.0.0.0 --port 5001 --reload

restart_dev_cluster:
	curl -X POST http://127.0.0.1:5001/restart_cluster

restart_test_cluster:
	curl -X POST -H "Content-Length: 0" https://cluster.test.burla.dev/restart_cluster

restart_prod_cluster:
	curl -X POST -H "Content-Length: 0" https://cluster.burla.dev/restart_cluster

test_node:
	poetry run python -c "from main_service.node import Node; Node.start('n4-standard-2')"

deploy-test:
	set -e; \
	TEST_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$(ARTIFACT_PKG_NAME) \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project=burla-test-joe \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	TEST_IMAGE_NAME=$$( echo $(TEST_IMAGE_BASE_NAME):$${TEST_IMAGE_TAG} ); \
	gcloud run deploy $(WEBSERVICE_NAME) \
	--image=$${TEST_IMAGE_NAME} \
	--project burla-test-joe \
	--region=us-central1 \
	--set-env-vars IN_PRODUCTION=False \
	--min-instances 0 \
	--max-instances 5 \
	--memory 2Gi \
	--cpu 1 \
	--timeout 360 \
	--concurrency 20 \
	--allow-unauthenticated

move-test-image-to-prod:
	set -e; \
	TEST_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$(ARTIFACT_PKG_NAME) \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project=burla-test-joe \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	TEST_IMAGE_NAME=$$( echo $(TEST_IMAGE_BASE_NAME):$${TEST_IMAGE_TAG} ); \
	PROD_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$(ARTIFACT_PKG_NAME) \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project=burla-prod \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	NEW_PROD_IMAGE_TAG=$$(($${PROD_IMAGE_TAG} + 1)); \
	PROD_IMAGE_NAME=$$( echo $(PROD_IMAGE_BASE_NAME):$${NEW_PROD_IMAGE_TAG} ); \
	docker pull $${TEST_IMAGE_NAME}; \
	docker tag $${TEST_IMAGE_NAME} $${PROD_IMAGE_NAME}; \
	docker push $${PROD_IMAGE_NAME}

deploy-prod:
	set -e; \
	echo ; \
	echo HAVE YOU MOVED THE LATEST TEST-IMAGE TO PROD?; \
	while true; do \
		read -p "Do you want to continue? (yes/no): " yn; \
		case $$yn in \
			[Yy]* ) echo "Continuing..."; break;; \
			[Nn]* ) echo "Exiting..."; exit;; \
			* ) echo "Please answer yes or no.";; \
		esac; \
	done; \
	PROD_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$(ARTIFACT_PKG_NAME) \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project burla-prod \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	PROD_IMAGE_NAME=$$( echo $(PROD_IMAGE_BASE_NAME):$${PROD_IMAGE_TAG} ); \
	gcloud run deploy $(WEBSERVICE_NAME) \
	--image=$${PROD_IMAGE_NAME} \
	--project burla-prod \
	--region=us-central1 \
	--min-instances 1 \
	--max-instances 20 \
	--memory 4Gi \
	--cpu 1 \
	--timeout 360 \
	--concurrency 20 \
	--allow-unauthenticated

image:
	set -e; \
	TEST_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$(ARTIFACT_PKG_NAME) \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project burla-test-joe \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	NEW_TEST_IMAGE_TAG=$$(($${TEST_IMAGE_TAG} + 1)); \
	TEST_IMAGE_NAME=$$( echo $(TEST_IMAGE_BASE_NAME):$${NEW_TEST_IMAGE_TAG} ); \
	gcloud builds submit --tag $${TEST_IMAGE_NAME}; \
	echo "Successfully built Docker Image:"; \
	echo "$${TEST_IMAGE_NAME}"; \
	echo "";

container:
	set -e; \
	TEST_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$(ARTIFACT_PKG_NAME) \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project burla-test-joe \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	TEST_IMAGE_NAME=$$( echo $(TEST_IMAGE_BASE_NAME):$${TEST_IMAGE_TAG} ); \
	docker run --rm -it \
		-v $(PWD):/home/pkg_dev/app \
		-v ~/.gitconfig:/home/pkg_dev/.gitconfig \
		-v ~/.ssh/id_rsa:/home/pkg_dev/.ssh/id_rsa \
		-v ~/.config/gcloud:/home/pkg_dev/.config/gcloud \
		-e GOOGLE_CLOUD_PROJECT=burla-test-joe \
		-e IN_DEV=True \
		-e IN_PRODUCTION=False \
		-p 5001:5001 \
		--entrypoint poetry $${TEST_IMAGE_NAME} run bash
