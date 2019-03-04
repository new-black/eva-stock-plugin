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


class EVAStockScriptEngine : ScriptEngine {
    override fun getType(): String {
        return "eva_scripts"
    }

    override fun <T> compile(scriptName: String, scriptSource: String,
                             context: ScriptContext<T>, params: Map<String, String>): T {
        if (context == FilterScript.CONTEXT == false) {
            throw IllegalArgumentException(type
                    + " scripts cannot be used for context ["
                    + context.name + "]")
        }
        // we use the script "source" as the script identifier

        val factory = FilterScript.Factory { params, lookup -> EVAStockFilterScriptFactory(params, lookup) }
        return context.factoryClazz.cast(factory)
    }

    override fun close() {
        // optionally close resources
    }

    private class EVAStockFilterScriptFactory(
        private val params: Map<String, Any>, private val lookup: SearchLookup) : FilterScript.LeafFactory {
        private val field: String
        private val term: String

        init {
            field = params["field"].toString()
            term = params["term"].toString()
        }

        @Throws(IOException::class)
        override fun newInstance(context: LeafReaderContext): FilterScript {
            return object : FilterScript(params, lookup, context) {
                override fun execute(): Boolean {

                    var values = doc["product_id"]

                    values!!.forEach {
                        println(it.toString())
                    }

                    return true
                }
            }
        }
    }
}