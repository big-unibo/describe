package it.unibo.web

import com.google.common.collect.Maps
import it.unibo.conversational.database.Config
import it.unibo.conversational.database.DBmanager
import it.unibo.describe.Describe
import it.unibo.describe.DescribeExecute
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.AgeFileFilter
import org.apache.commons.lang3.time.DateUtils
import org.json.JSONObject
import java.io.File
import java.util.*
import javax.servlet.ServletException
import javax.servlet.annotation.WebServlet
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * Servlet interface for intentions.
 */
@WebServlet("/Intention")
class IntentionServlet : HttpServlet() {

    @get:Synchronized
    var id = 0
        get() = field++
        private set

    /**
     * Given a sentence returns the string representing the parsing tree.
     *
     * @param request  request
     * @param response response
     * @throws ServletException in case of error
     * @see HttpServlet.doGet
     */
    @Throws(ServletException::class)
    override fun doGet(request: HttpServletRequest, response: HttpServletResponse) {
        println(PYTHON_PATH)
        val result: JSONObject
        val error = JSONObject()
        val status: Int

        try {
            cleanOldFiles()
            // read the inputs
            val sessionid = request.getParameter("sessionid")
            val version = request.getParameter("version").toString()
            error.put("sessionid", sessionid)
            error.put("version", version)
            val value: String = manipulateInString(request.getParameter("value"))
            error.put("value", value)
            if (!empty(value)) {
                status = OK
                val session = sessions[sessionid]
                if (session == null) {
                    DescribeExecute.Vcoord.clear()
                    DescribeExecute.Vmemb.clear()
                }
                val d = DescribeExecute.parse(sessions[sessionid], value, false)
                result = DescribeExecute.execute(d, servletContext.getRealPath("WEB-INF/classes/"), PYTHON_PATH, makePivot = true, version.equals("adbis")).first
                sessions[if (empty(sessionid)) id.toString() + "" else sessionid] = d
            } else {
                status = ERROR
                result = JSONObject()
                error.put("error", "Empty string")
            }
            write(response, if (status == OK) manipulateOutString(result) else error.toString())
        } catch (ex: Exception) {
            ex.printStackTrace()
            try {
                error.put("error", ex.localizedMessage)
                write(response, error.toString())
            } catch (e: Exception) {
                // e.printStackTrace()
            }
        }
        DBmanager.closeAllConnections()
    }

    /**
     * Given a sentence returns the string representing the parsing tree.
     *
     * @param request
     * request
     * @param response
     * response
     * @throws ServletException
     * in case of error
     */
    @Throws(ServletException::class)
    override fun doPost(request: HttpServletRequest, response: HttpServletResponse) {
        doGet(request, response)
    }

    companion object {
        private const val OK = 200
        private const val ERROR = 500
        val PYTHON_PATH: String = Config.getPython()
    }

    // Stored sessions
    val sessions: MutableMap<String, Describe> = Maps.newLinkedHashMap()

    /**
     * Check if empty value
     * @param value value
     */
    private fun empty(value: String?): Boolean = value.isNullOrEmpty()

    /**
     * Send the result
     * @param response HTTP response object
     * @param result result
     */
    fun write(response: HttpServletResponse, result: String) {
        response.addHeader("Access-Control-Allow-Origin", "*")
        response.addHeader("Access-Control-Allow-Methods", "GET, POST, PATCH, PUT, DELETE, OPTIONS")
        response.addHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, X-Auth-Token")
        response.characterEncoding = "UTF-8"
        response.status = OK
        response.outputStream.print(result)
    }

    /**
     * Clean old useless files, to avoid to burden the server too much
     */
    fun cleanOldFiles() {
        /* **********************************************************************
         * CLEAN OLD .CSV FILES
         ********************************************************************** */
        val oldestDate = DateUtils.addMinutes(Date(), -30) // Remove file older than 30 minutes
        val targetDir = File(servletContext.getRealPath("WEB-INF/classes"))
        val filesToDelete = FileUtils.iterateFiles(targetDir, AgeFileFilter(oldestDate), null)
        while (filesToDelete.hasNext()) {
            val toDelete = filesToDelete.next()
            if (toDelete.name.endsWith(".csv")) {
                FileUtils.deleteQuietly(toDelete)
            }
        }
    }

    /**
     * Manipulate the string if needed (e.g., to use more user-friendly names)
     * @param v string tu manipulate
     */
    fun manipulateInString(v: String): String {
        var value = v
        if (v.lowercase().startsWith("with sales ")) {
            value = "$value " //
                    .replace(",", " , ") //
                    .replace(" customer ", " customer_id ") //
                    .replace(" product ", " product_name ") //
                    .replace(" date ", " the_date ") //
                    .replace(" month ", " the_month ") //
                    .replace(" year ", " the_year ") //
                    .replace(" city ", " store_city ") //
                    .replace(" country ", " store_country ") //
                    .replace(" category ", " product_category ") //
                    .replace(" type ", " product_subcategory ") //
                    .replace(" quantity ", " unit_sales ") //
                    .replace(" storeSales ", " store_sales ") //
                    .replace(" storeCost ", " store_cost ") //
                    .replace(" store ", " store_id ")
            return value
        }
        return value
    }

    /**
     * Manipulate the output if needed (e.g., to use more user-friendly names)
     * @param result json object to manipulate
     */
    fun manipulateOutString(result: JSONObject): String {
        return result.toString() //
                .replace("customer_id", "customer") //
                .replace("product_id", "product") //
                .replace("product_name", "product") //
                .replace("store_id", "store") //
                .replace("the_date", "date") //
                .replace("the_month", "month") //
                .replace("the_year", "year") //
                .replace("store_city", "city") //
                .replace("store_country", "country") //
                .replace("product_subcategory", "type") //
                .replace("product_category", "category") //
                .replace("unit_sales", "quantity") //
                .replace("store_sales", "storeSales") //
                .replace("store_cost", "storeCost") //
    }
}