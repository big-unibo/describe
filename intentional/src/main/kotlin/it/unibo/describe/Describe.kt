package it.unibo.describe

import com.google.common.base.Optional
import com.google.common.collect.Sets
import it.unibo.Intention
import java.io.File

class Describe : Intention {
    private var models: Set<String> = Sets.newLinkedHashSet()
    var k: Optional<Int> = Optional.absent()

    constructor(d: Describe?, accumulateAttributes: Boolean) : super(d, accumulateAttributes)
    constructor(accumulateAttributes: Boolean) : super(null, accumulateAttributes)

    private fun getModels(): Set<String> {
        return models.ifEmpty { Sets.newHashSet("top-k", "bottom-k", "clustering", "outliers", "skyline") }
    }

    fun setModels(models: List<String>) {
        this.models = models.toSet()
    }

    override fun toPythonCommand(commandPath: String, path: String): String {
        val sessionStep = getSessionStep()
        val filename = getFilename()
        val fullCommand = (commandPath.replace("/", File.separator) //
                + " --path " + (if (path.contains(" ")) "\"" else "") + path.replace("\\", "/") + (if (path.contains(" ")) "\"" else "") //
                + " --file " + filename //
                + " --session_step " + sessionStep //
                + (if (k.isPresent) " --k " + k.get() else "") //
                + " --computeproperty " + (if (computeProperty) "True" else "False") //
                + " --models " + getModels().stream().reduce("") { a: String, b: String -> "$a $b" } //
                + " --cube " + json.toString().replace(" ", "__"))
        L.warn(fullCommand)
        return fullCommand
    }

    companion object {
        var computeProperty = true
    }
}