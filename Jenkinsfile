pipeline {
    agent any

    tools {
        jdk 'JDK 21'
        maven 'Maven 3'
    }

    options {
        skipDefaultCheckout(true)
    }

    triggers {
        // Fallback if a webhook is missed. Multibranch + "GitHub Branch Source"
        // is what actually discovers and builds pull requests automatically.
        pollSCM('H/5 * * * *')
    }

    environment {
        GITHUB_REPO = 'RNIS-TW/MCGate'
        // Jenkins credentials id for a GitHub token with repo:status + PR comment scope.
        GITHUB_CRED = 'github-rnis'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
                script {
                    env.GIT_SHA = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
                }
                // "loader icon" on the commit / PR in GitHub -> pending status check
                githubNotify context: 'ci/jenkins',
                             status: 'PENDING',
                             description: 'Build started',
                             repo: env.GITHUB_REPO,
                             sha: env.GIT_SHA,
                             credentialsId: env.GITHUB_CRED
            }
        }

        stage('Build') {
            steps {
                sh 'mvn -B clean compile'
            }
        }

        stage('Test') {
            steps {
                sh 'mvn -B test'
            }
            post {
                always {
                    junit testResults: 'target/surefire-reports/*.xml', allowEmptyResults: true
                    sh 'cd target && zip -qr test-reports.zip surefire-reports || true'
                    archiveArtifacts artifacts: 'target/test-reports.zip', allowEmptyArchive: true, fingerprint: true
                }
            }
        }

        stage('Package') {
            steps {
                sh 'mvn -B package -DskipTests'
            }
            post {
                success {
                    archiveArtifacts artifacts: 'target/MCGate-*.jar', fingerprint: true
                }
            }
        }
    }

    post {
        success {
            githubNotify context: 'ci/jenkins', status: 'SUCCESS', description: 'Build passed',
                         repo: env.GITHUB_REPO, sha: env.GIT_SHA, credentialsId: env.GITHUB_CRED
            commentOnPr('✅')
        }
        failure {
            githubNotify context: 'ci/jenkins', status: 'FAILURE', description: 'Build failed',
                         repo: env.GITHUB_REPO, sha: env.GIT_SHA, credentialsId: env.GITHUB_CRED
            commentOnPr('❌')
        }
    }
}

// Posts a build summary on the PR with a link to the archived test-reports.zip.
// GitHub's comment API can't hold binary attachments, so we attach it to the
// build and link it from the comment.
def commentOnPr(String icon) {
    if (!env.CHANGE_ID) {
        return
    }
    def tests = junitResultSummary()
    def reportUrl = "${env.BUILD_URL}artifact/target/test-reports.zip"
    def body = """\
${icon} **Jenkins build [#${env.BUILD_NUMBER}](${env.BUILD_URL})** — `${env.GIT_SHA?.take(7)}`

| Result | Total | Failed | Skipped |
|---|---|---|---|
| ${currentBuild.currentResult} | ${tests.total} | ${tests.failed} | ${tests.skipped} |

📎 [Download test report (test-reports.zip)](${reportUrl})
"""
    pullRequest.comment(body)
}

def junitResultSummary() {
    def action = currentBuild.rawBuild.getAction(hudson.tasks.junit.TestResultAction.class)
    if (action == null) {
        return [total: 0, failed: 0, skipped: 0]
    }
    return [total: action.totalCount, failed: action.failCount, skipped: action.skipCount]
}
