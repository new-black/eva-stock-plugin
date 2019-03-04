package io.newblack.elastic

import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.NamedWriteableRegistry
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.plugins.ScriptPlugin
import org.elasticsearch.script.*
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.watcher.ResourceWatcherService

class EVAStockPlugin : Plugin(), ScriptPlugin {

    private val logger = Loggers.getLogger(EVAStockPlugin::class.java, "eva")

    private lateinit var scheduler: ReloadStockScheduler

    override fun createComponents(client: Client?, clusterService: ClusterService?, threadPool: ThreadPool?, resourceWatcherService: ResourceWatcherService?, scriptService: ScriptService?, xContentRegistry: NamedXContentRegistry?, environment: Environment?, nodeEnvironment: NodeEnvironment?, namedWriteableRegistry: NamedWriteableRegistry?): MutableCollection<Any> {
        scheduler = ReloadStockScheduler(clusterService!!.settings, threadPool!!.scheduler())

        return mutableListOf(scheduler)
    }

    override fun getScriptEngine(settings: Settings?, contexts: MutableCollection<ScriptContext<*>>?): ScriptEngine {
        return EVAScriptEngine()
    }
}
