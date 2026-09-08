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
        // Fallback if a webhook is missed. A Multibranch Pipeline job with the
        // "GitHub Branch Source" plugin is what discovers/builds PRs automatically.
        pollSCM('H/5 * * * *')
    }

    environment {
        GITHUB_ACCOUNT = 'RNIS-TW'
        GITHUB_REPO = 'MCGate'
        // Must be a GLOBAL-scoped "Username with password" credential
        // (username = GitHub user, password = PAT with commit-status scope).
        // The githubNotify step does NOT recognise "Secret text" credentials.
        GITHUB_CRED = 'github-rnis'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
                script {
                    env.GIT_SHA = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
                }
                notifyGitHub('PENDING', 'Build started')
            }
        }

        stage('Build') {
            steps {
                sh 'mvn -B clean compile'
            }
        }

        stage('Test') {
            steps {
                // pom.xml sets <skipTests>true</skipTests> so a plain `mvn package` build stays
                // fast; CI must override it or this stage silently runs zero tests.
                sh 'mvn -B test -DskipTests=false'
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
            script {
                notifyGitHub('SUCCESS', 'Build passed')
                commentOnPr('✅')
            }
        }
        failure {
            script {
                notifyGitHub('FAILURE', 'Build failed')
                commentOnPr('❌')
            }
        }
    }
}

// GitHub commit status = the pending/success/failure "loader icon" on the commit.
// Non-fatal: a missing plugin or credential just logs a warning.
def notifyGitHub(String status, String description) {
    try {
        githubNotify context: 'ci/jenkins',
                     status: status,
                     description: description,
                     repo: env.GITHUB_REPO,
                     account: env.GITHUB_ACCOUNT,
                     sha: env.GIT_SHA,
                     credentialsId: env.GITHUB_CRED
    } catch (e) {
        echo "githubNotify skipped: ${e.message}"
    }
}

// Posts a build summary on the PR with a link to the archived test-reports.zip.
// Requires the "Pipeline: GitHub" plugin (provides the `pullRequest` global) and
// a Multibranch Pipeline job (so env.CHANGE_ID is set on PR builds).
// GitHub's comment API can't hold binary attachments, so we archive the report
// on the build and link it.
def commentOnPr(String icon) {
    if (!env.CHANGE_ID) {
        echo 'Not a PR build (no CHANGE_ID) - skipping PR comment.'
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
    try {
        pullRequest.comment(body)
    } catch (NoSuchMethodError | MissingPropertyException e) {
        echo "Could not post PR comment (install the 'Pipeline: GitHub' plugin): ${e.message}"
    }
}

def junitResultSummary() {
    def action = currentBuild.rawBuild.getAction(hudson.tasks.junit.TestResultAction.class)
    if (action == null) {
        return [total: 0, failed: 0, skipped: 0]
    }
    return [total: action.totalCount, failed: action.failCount, skipped: action.skipCount]
}
