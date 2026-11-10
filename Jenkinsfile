pipeline {
  agent any

  environment {
    IMAGE_NAME     = 'my-springboot-app'
    CONTAINER_NAME = 'my-springboot-app'
  }

  triggers {
    pollSCM('H/2 * * * *')   // check Git every 2 minutes
  }

  stages {
    stage('Build') {
      steps {
        bat 'mvnw.cmd clean package -DskipTests'
      }
    }

    stage('Test') {
      steps {
        bat 'mvnw.cmd test'
      }
      post {
        always {
          junit allowEmptyResults: true, testResults: 'target/surefire-reports/*.xml'
        }
      }
    }

    stage('Docker Build') {
      steps {
        bat "docker build -t %IMAGE_NAME%:%BUILD_NUMBER% -t %IMAGE_NAME%:latest ."
      }
    }

    stage('Deploy') {
      steps {
        bat "docker rm -f %CONTAINER_NAME% || exit 0"
        bat "docker run -d --name %CONTAINER_NAME% -p 8081:8080 %IMAGE_NAME%:latest"
      }
    }
  }

  post {
    success { echo 'Pipeline OK. App running on http://localhost:8081' }
    failure { echo 'Pipeline failed. Check the Console Output.' }
  }
}