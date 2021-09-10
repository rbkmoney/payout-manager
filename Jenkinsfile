#!groovy
build('payout-manager', 'java-maven') {
    checkoutRepo()
    loadBuildUtils()

    def javaServicePipeline
    runStage('load JavaService pipeline') {
        javaServicePipeline = load("build_utils/jenkins_lib/pipeJavaServiceInsideDocker.groovy")
    }

    def serviceName = env.REPO_NAME
    def mvnArgs = '-DjvmArgs="-Xmx256m"'
    def registry = 'dr2.rbkmoney.com'
    def registryCredsId = 'jenkins_harbor'

    javaServicePipeline(serviceName, mvnArgs)
}
