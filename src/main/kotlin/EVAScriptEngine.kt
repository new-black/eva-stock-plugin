package io.newblack.elastic

import com.carrotsearch.hppc.LongIntHashMap
import org.elasticsearch.script.ScriptContext
import org.elasticsearch.script.ScriptEngine
import org.elasticsearch.script.SearchScript
import java.io.UncheckedIOException
import java.io.IOException
import org.apache.lucene.index.PostingsEnum
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.Term
import org.elasticsearch.script.FilterScript
import org.elasticsearch.search.aggregations.support.values.ScriptLongValues
import org.elasticsearch.search.lookup.SearchLookup
import org.elasticsearch.script.ScoreScript
import org.apache.logging.log4j.ThreadContext.containsKey
import org.elasticsearch.index.fielddata.ScriptDocValues

class EVAScriptEngine : ScriptEngine {
    override fun getType(): String {
        return "native"
    }

    override fun <T> compile(scriptName: String, scriptSource: String,
                             context: ScriptContext<T>, params: Map<String, String>): T {

        if (scriptSource == "filter_stock") {
            val factory = FilterScript.Factory(::EVAStockFilterScriptFactory)
            return context.factoryClazz.cast(factory)
        } else if (scriptSource == "sort_stock") {

            if(context.factoryClazz.isAssignableFrom(ScoreScript.Factory::class.java)) {
                val factory = ScoreScript.Factory(::EVAStockScoreScriptFactory)
                return context.factoryClazz.cast(factory)
            } else {
                val factory = SearchScript.Factory(::EVAStockSortScriptFactory)
                return context.factoryClazz.cast(factory)
            }
        }

        throw java.lang.IllegalArgumentException("Unknown script")
    }

    override fun close() {}

    private class EVAStockSortScriptFactory(private val params: Map<String, Any>,
                                            private val lookup: SearchLookup) : SearchScript.LeafFactory {
        override fun needs_score(): Boolean {
            return false
        }

        val orgIDsParam = params["organization_unit_ids"]

        val orgIDs = if (orgIDsParam is ArrayList<*>) {
            orgIDsParam.map { (it as Int).toLong() }.toLongArray()
        } else {
            LongArray(0)
        }

        val boostAmountParam = params["boost_amount"]

        val boostAmount = when (boostAmountParam) {
            is Int -> boostAmountParam.toDouble()
            is Double -> boostAmountParam
            is String -> boostAmountParam.toDouble()
            else -> {
                1.0
            }
        }

        private class Script(private val orgIDs: LongArray,
                             private val boostAmount: Double,
                             private val stock: LongIntHashMap, params: Map<String, Any>,
                             lookup: SearchLookup, leafContext: LeafReaderContext)
            : SearchScript(params, lookup, leafContext) {
            override fun runAsDouble(): Double {
                val productID = (doc["product_id"] as ScriptDocValues.Longs)[0]

                for (orgID in orgIDs) {

                    val key = productID shl 32 or (orgID and 0xffffffffL)

                    val hasStock = stock.containsKey(key)

                    if (hasStock) return boostAmount
                }

                return 0.0
            }

        }

        @Throws(IOException::class)
        override fun newInstance(context: LeafReaderContext): SearchScript {

            val stock = ReloadStockScheduler.stockMap

            return Script(orgIDs, boostAmount, stock, params, lookup, context)
        }
    }

    private class EVAStockScoreScriptFactory(private val params: Map<String, Any>,
                                            private val lookup: SearchLookup) : ScoreScript.LeafFactory {
        override fun needs_score(): Boolean {
            return false
        }

        val orgIDsParam = params["organization_unit_ids"]

        val orgIDs = if (orgIDsParam is ArrayList<*>) {
            orgIDsParam.map { (it as Int).toLong() }.toLongArray()
        } else {
            LongArray(0)
        }

        val boostAmountParam = params["boost_amount"]

        val boostAmount = when (boostAmountParam) {
            is Int -> boostAmountParam.toDouble()
            is Double -> boostAmountParam
            is String -> boostAmountParam.toDouble()
            else -> {
                1.0
            }
        }

        private class Script(private val orgIDs: LongArray,
                             private val boostAmount: Double,
                             private val stock: LongIntHashMap, params: Map<String, Any>,
                             lookup: SearchLookup, leafContext: LeafReaderContext)
            : ScoreScript(params, lookup, leafContext) {
            override fun execute(): Double {
                val productID = (doc["product_id"] as ScriptDocValues.Longs)[0]

                for (orgID in orgIDs) {

                    val key = productID shl 32 or (orgID and 0xffffffffL)

                    val hasStock = stock.containsKey(key)

                    if (hasStock) return boostAmount
                }

                return 0.0
            }
        }

        @Throws(IOException::class)
        override fun newInstance(context: LeafReaderContext): ScoreScript {

            val stock = ReloadStockScheduler.stockMap

            return Script(orgIDs, boostAmount, stock, params, lookup, context)
        }
    }

    private class EVAStockFilterScriptFactory(private val params: Map<String, Any>,
                                              private val lookup: SearchLookup) : FilterScript.LeafFactory {
        val orgIDsParam = params["organization_unit_ids"]

        val orgIDs = if (orgIDsParam is ArrayList<*>) {
            orgIDsParam.map { (it as Int).toLong() }.toLongArray()
        } else {
            LongArray(0)
        }

        private class Script(private val orgIDs: LongArray,
                             private val stock: LongIntHashMap, params: Map<String, Any>,
                             lookup: SearchLookup, leafContext: LeafReaderContext)
            : FilterScript(params, lookup, leafContext) {

            override fun execute(): Boolean {

                val productID = (doc["product_id"] as ScriptDocValues.Longs)[0]

                for (orgID in orgIDs) {

                    val key = productID shl 32 or (orgID and 0xffffffffL)

                    if (stock.containsKey(key)) {
                        return true
                    }
                }

                return false
            }
        }

        @Throws(IOException::class)
        override fun newInstance(context: LeafReaderContext): FilterScript {

            val stock = ReloadStockScheduler.stockMap

            return Script(orgIDs, stock, params, lookup, context)
        }
    }
}