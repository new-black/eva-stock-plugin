package io.newblack.elastic

import com.carrotsearch.hppc.LongIntHashMap
import jdk.internal.util.EnvUtils
import org.apache.http.HttpStatus
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.lucene.analysis.synonym.SynonymMap
import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.Index
import org.omg.CORBA.Environment
import java.io.ByteArrayInputStream
import java.lang.Exception
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.AccessController
import java.security.PrivilegedAction
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class ReloadStockScheduler(
        settings: Settings,
        private val scheduler: ScheduledExecutorService
) : AbstractLifecycleComponent(settings) {

    private var schedule: ScheduledFuture<*>? = null
    private val logger = Loggers.getLogger(ReloadStockScheduler::class.java, "eva")

    companion object {
        public var stockMap = LongIntHashMap()
    }

    override fun doStart() {

        var url = System.getenv("EVA_STOCK_URL") ?: "http://eva-backend-service:8080/middleware/pim/stock"

        var reloadTimeString = System.getenv("STOCK_REFRESH_INTERVAL_MINUTES")

        var reloadTime = reloadTimeString?.toLongOrNull() ?: 60L

        schedule = scheduler.scheduleAtFixedRate({

            AccessController.doPrivileged(PrivilegedAction<Unit> {
                logger.info("Reloading Stock from $url")

                var config = RequestConfig.custom()
                        .setConnectTimeout(10 * 1000)
                        .setSocketTimeout(30 * 1000)
                        .setConnectionRequestTimeout(5 * 1000).build()

                var httpClientFactory = HttpClients.custom().setDefaultRequestConfig(config)

                try {
                    httpClientFactory.build().use {

                        var request = HttpGet(url)

                        it.execute(request).use { httpResponse ->

                            logger.info("Received response with Status ${httpResponse.statusLine.statusCode}")

                            if (httpResponse.statusLine.statusCode == HttpStatus.SC_OK) {

                                var data = httpResponse.entity.content.readBytes()

                                logger.info("Response size ${data.size}")

                                val buffer = ByteBuffer.wrap(data)

                                buffer.order(ByteOrder.LITTLE_ENDIAN)

                                val map = LongIntHashMap(data.size / 12)

                                for (i in 0 until data.size step 12) {

                                    val productID = buffer.int
                                    val orgID = buffer.int
                                    val onHand = buffer.int

                                    val key = productID.toLong() shl 32 or (orgID.toLong() and 0xffffffffL)

                                    map.put(key, onHand)
                                }

                                stockMap = map

                            } else {
                                logger.warn("Not reloading stock as response code is ${httpResponse.statusLine.statusCode}")
                            }
                        }

                    }
                } catch (ex: Exception) {
                    logger.warn("Caught exception reloading stock ${ex.message}")
                }

                logger.info("Reloaded Stock: ${stockMap.size()} records")
            })

        }, 0L, reloadTime, TimeUnit.MINUTES)
    }

    override fun doStop() {
        schedule?.cancel(false)
    }

    override fun doClose() {}

}