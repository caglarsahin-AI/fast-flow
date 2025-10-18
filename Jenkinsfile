pipeline {
	agent {
		node {
			label "Slave5"
		}
	}
    environment {
        BASE_IMAGE = "apache/airflow:2.7.2-python3.9"
        BASE_IMAGE_VERSION = "2.7.2-python3.9"
        REGISTRY_URL = "artifacts.paycore.com"
        IMAGE_NAME = "apache/airflow"
        DOCKER_CREDENTIALS_ID = "docker-publisher"
        ENVIRONMENT="test"
    }
    stages {
        stage('Build Docker Image') {
            steps {
                script {
                    // Dockerfile'da ARG olarak BASE_IMAGE kullanılıyor kabul edilmiştir
                    def tag = env.BUILD_NUMBER ?: "latest"
                    sh """
                    docker build --build-arg BASE_IMAGE=${BASE_IMAGE} -t ${REGISTRY_URL}/${IMAGE_NAME}:${BASE_IMAGE_VERSION}.${BUILD_NUMBER}-${ENVIRONMENT} .
                    """
                }
            }
        }

        stage('Push Docker Image') {
            steps {
                script {
                    def tag = env.BUILD_NUMBER ?: "latest"
                    docker.withRegistry("https://${REGISTRY_URL}", "${DOCKER_CREDENTIALS_ID}") {
                        sh "docker push ${REGISTRY_URL}/${IMAGE_NAME}:${BASE_IMAGE_VERSION}.${BUILD_NUMBER}-${ENVIRONMENT}"
                    }
                }
            }
        }
    }
}
