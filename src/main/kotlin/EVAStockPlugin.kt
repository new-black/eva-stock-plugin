package io.newblack.elastic

import com.carrotsearch.hppc.LongIntHashMap
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
        return EVAStockScriptEngine()
    }

//    override fun getNativeScripts(): MutableList<NativeScriptFactory> {
//        return mutableListOf(FilterStockScriptFactory(), SortStockScriptFactory())
//    }

//    class SortStockScriptFactory : NativeScriptFactory {
//        override fun needsScores(): Boolean {
//            return true
//        }
//
//        override fun newScript(params: MutableMap<String, Any>?): ExecutableScript {
//
//            var orgIDsParam = params?.get("organization_unit_ids")
//
//            val orgIDs =  if (orgIDsParam is ArrayList<*>) {
//                orgIDsParam.map { (it as Int).toLong() }.toLongArray()
//            } else {
//                LongArray(0)
//            }
//
//            var boostAmountParam = params?.get("boost_amount")
//
//            val boostAmount = when (boostAmountParam) {
//                is Int -> boostAmountParam.toDouble()
//                is Double -> boostAmountParam
//                is String -> boostAmountParam.toDouble()
//                else -> {
//                    1.0
//                }
//            }
//
//            return SortStockScript(orgIDs, ReloadStockScheduler.stockMap, boostAmount)
//        }
//
//        override fun getName(): String {
//            return "sort_stock"
//        }
//
//        class SortStockScript(private val orgIDs: LongArray, private val stock: LongIntHashMap, private val boostAmount : Double) : AbstractDoubleSearchScript() {
//            override fun runAsDouble(): Double {
//
//                val productID = docFieldLongs("product_id")[0]
//
//                for (orgID in orgIDs) {
//
//                    val key = productID shl 32 or (orgID and 0xffffffffL)
//
//                    val hasStock = stock.containsKey(key)
//
//                    if (hasStock) return boostAmount
//                }
//
//                return 0.0
//
//            }
//        }
//    }
//
//    class FilterStockScriptFactory : NativeScriptFactory {
//        override fun needsScores(): Boolean {
//            return false
//        }
//
//        override fun newScript(params: MutableMap<String, Any>?): ExecutableScript {
//
//            var orgIDsParam = params?.get("organization_unit_ids")
//
//            val orgIDs =  if (orgIDsParam is ArrayList<*>) {
//                orgIDsParam.map { (it as Int).toLong() }.toLongArray()
//            } else {
//                LongArray(0)
//            }
//
//            return FilterStockScript(orgIDs, ReloadStockScheduler.stockMap)
//        }
//
//        override fun getName(): String {
//            return "filter_stock"
//        }
//
//        class FilterStockScript(private val orgIDs: LongArray, private val stock: LongIntHashMap) : AbstractSearchScript() {
//
//            override fun run(): Any {
//
//                val productID = docFieldLongs("product_id")[0]
//
//                for (orgID in orgIDs) {
//
//                    val key = productID shl 32 or (orgID and 0xffffffffL)
//
//                    val hasStock = stock.containsKey(key)
//
//                    if (hasStock) return true
//                }
//
//                return false
//            }
//        }
//    }
}
