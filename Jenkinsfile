pipeline {
    agent any
    
    stages {
        stage('Test') {
            // TODO: Add tests
        }
        
        stage('Static Code Analysis') {
            steps {
                script {
                    def scannerHome = tool 'SonarScanner';
                    withSonarQubeEnv() {
                        sh """
                            ${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=fonda-airflow-dags
                        """
                    }
                }
            }
        }
    }
}