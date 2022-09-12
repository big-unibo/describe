package it.unibo.describe

import com.google.common.base.Optional
import it.unibo.antlr.gen.DescribeBaseListener
import it.unibo.antlr.gen.DescribeParser
import it.unibo.antlr.gen.DescribeParser.CContext
import it.unibo.antlr.gen.DescribeParser.DescribeContext
import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.tree.ErrorNode
import org.apache.commons.lang3.tuple.Triple
import java.util.stream.Collectors

/**
 * How to interpret a Describe syntax
 */
class DescribeListenerCustom(d: Describe?, accumulateAttributes: Boolean) : DescribeBaseListener() {
    var describe: Describe? = null

    override fun visitErrorNode(node: ErrorNode) {
        throw IllegalArgumentException("Invalid describe syntax")
    }

    override fun exitC(ctx: CContext) {
        describe!!.setCube(ctx.cube.name)
    }

    override fun exitDescribe(ctx: DescribeContext) {
        describe!!.setMeasures(ctx.mc.stream().map { obj: DescribeParser.IdContext -> obj.text }.collect(Collectors.toList()))
        if (ctx.gc != null) {
            // describe.setAttribute(ctx.gc.stream().map(t -> t.name).collect(Collectors.toList()).toArray());
            describe!!.addAttribute(true, *ctx.gc.stream().map { t: DescribeParser.IdContext -> t.name }.toArray())
        }
        if (ctx.models != null) {
            describe!!.setModels(ctx.models.stream().map { obj: Token -> obj.text }.collect(Collectors.toList()))
        }
        if (ctx.k != null) {
            describe!!.k = Optional.of(ctx.k.text.toInt())
        }
    }

    override fun exitCondition(ctx: DescribeParser.ConditionContext) {
        describe!!.addClause(Triple.of(ctx.attr.text, if (ctx.op == null) ctx.`in`.text else ctx.op.text, ctx.`val`.stream().map { obj: DescribeParser.ValueContext -> obj.text }.collect(Collectors.toList())))
    }

    init {
        describe =
                if (d == null) {
                    Describe(accumulateAttributes)
                } else {
                    Describe(d, accumulateAttributes)
                }
    }
}