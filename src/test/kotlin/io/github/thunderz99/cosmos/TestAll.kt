package io.github.thunderz99.cosmos

import java.io.PrintWriter
import org.junit.platform.engine.discovery.ClassNameFilter.*
import org.junit.platform.engine.discovery.DiscoverySelectors.selectPackage
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder
import org.junit.platform.launcher.core.LauncherFactory
import org.junit.platform.launcher.listeners.SummaryGeneratingListener

// To start all test in VSCode debug 
fun main(args: Array<String>) {
    val launcher = LauncherFactory.create()
    val summary = SummaryGeneratingListener()
    launcher.registerTestExecutionListeners(summary)
    val request = LauncherDiscoveryRequestBuilder
            .request()
            .selectors(selectPackage("io.github.thunderz99.cosmos"))
            .build()
    launcher.execute(request)
    summary.summary.printFailuresTo(PrintWriter(System.out))
    summary.summary.printTo(PrintWriter(System.out))
}
