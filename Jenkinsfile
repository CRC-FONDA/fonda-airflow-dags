pipeline {
    agent {
        kubernetes {
            yamlFile 'jenkins-pod.yaml'
        }
    }
    
    stages {
        stage('Test') {
            container('python') {
                steps {
                    sh 'pip install -r requirements.txt'
                    sh 'py.test --junitxml test-results.xml dags/s1/force/test.py'
                }
                post {
                    always {
                        junit 'test-results.xml'
                        archiveArtifacts 'test-results.xml'
                    }
                }
            }
        }

        stage('Static Code Analysis') {
            steps {
                script {
                    def scannerHome = tool 'SonarScanner';
                    withSonarQubeEnv() {
                        sh """
                            ${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=fonda-airflow-dags \
                                -Dsonar.branch.name=$BRANCH_NAME
                        """
                    }
                }
            }
        }
    }
}