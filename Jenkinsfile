pipeline {
  agent any

  environment {
    IMAGE_NAME     = 'distribkv'
    CONTAINER_NAME = 'distribkv'
  }

  stages {
    stage('Test') {
      steps {
        bat 'mvnw.cmd clean test'
      }
      post {
        always {
          junit allowEmptyResults: true, testResults: '**/target/surefire-reports/*.xml'
        }
      }
    }

    stage('Package') {
      steps {
        bat 'mvnw.cmd package -DskipTests'
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
    success { echo 'Pipeline OK' }
    failure { echo 'Pipeline failed. Check the Console Output.' }
  }
}