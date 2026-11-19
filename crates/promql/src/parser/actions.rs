//! The goyacc driver (port of the `yyParse` code that goyacc writes into `generated_parser.y.go`) and the
//! semantic actions of `generated_parser.y`, with the parser helpers of `parse.go` they call.

use std::borrow::Cow;
use std::sync::Arc;

use model::histogram::{CounterResetHint, FloatHistogram, Span};
use model::labels::{self, Label, Labels, METRIC_NAME, MatchType, Matcher};

use super::ast::*;
use super::duration::{duration_seconds, go_f64_to_i64, parse_duration, seconds_to_duration};
use super::functions::{Function, FunctionMap, get_function};
use super::generated::*;
use super::lex::{Item, ItemType, Lexer};
use super::posrange::{Pos, PositionRange};
use super::quote::{go_quote, parse_float, parse_int_base0, parse_uint, unquote};
use super::value::ValueType;
use super::{Options, ParseErr, ParseResult, SequenceValue, Start};

/// The partial `BinaryExpr` that the modifier rules build (Go uses a `*BinaryExpr` without operands).
#[derive(Default)]
pub(crate) struct BinMod {
    vector_matching: VectorMatching,
    return_bool: bool,
}

/// A value of a histogram description (Go `map[string]any` values).
#[derive(Clone)]
enum DescVal {
    Int(i64),
    Float(f64),
    Floats(Vec<f64>),
    Hint(ItemType),
}

/// A histogram description: key and value pairs.
#[derive(Clone, Default)]
pub(crate) struct HistDesc(Vec<(&'static str, DescVal)>);

impl HistDesc {
    fn get(&self, k: &str) -> Option<&DescVal> {
        self.0.iter().find(|(key, _)| *key == k).map(|(_, v)| v)
    }

    fn one(k: &'static str, v: DescVal) -> HistDesc {
        HistDesc(vec![(k, v)])
    }
}

/// The node values of the grammar (Go `Node` in the `node` field of `yySymType`).
pub(crate) enum Node {
    Expr(Expr),
    Exprs(Vec<Expr>),
    AggMod(Box<AggregateExpr>),
    BinMod(Box<BinMod>),
}

/// A semantic value (Go `yySymType`, of which each symbol uses one field).
#[derive(Default)]
pub(crate) enum Sym<'a> {
    #[default]
    None,
    Node(Node),
    Item(Item<'a>),
    Matchers(Vec<Matcher>),
    Matcher(Option<Matcher>),
    Label(Label),
    Labels(Labels),
    LblList(Vec<Label>),
    Strings(Vec<String>),
    Series(Vec<SequenceValue>),
    Histogram(Box<FloatHistogram>),
    Descriptors(HistDesc),
    BucketSet(Vec<f64>),
    Int(i64),
    Uint(u64),
    Float(f64),
}

fn take<'a>(s: &mut Sym<'a>) -> Sym<'a> {
    std::mem::take(s)
}

fn item<'a>(s: &mut Sym<'a>) -> Item<'a> {
    match take(s) {
        Sym::Item(i) => i,
        _ => Item::default(),
    }
}

fn node(s: &mut Sym<'_>) -> Option<Node> {
    match take(s) {
        Sym::Node(n) => Some(n),
        _ => None,
    }
}

fn expr(s: &mut Sym<'_>) -> Expr {
    match node(s) {
        Some(Node::Expr(e)) => e,
        // Not reachable: the grammar gives every expr symbol an expression.
        _ => Expr::NumberLiteral(Box::default()),
    }
}

fn exprs(s: &mut Sym<'_>) -> Vec<Expr> {
    match node(s) {
        Some(Node::Exprs(e)) => e,
        _ => Vec::new(),
    }
}

fn strings(s: &mut Sym<'_>) -> Vec<String> {
    match take(s) {
        Sym::Strings(v) => v,
        _ => Vec::new(),
    }
}

fn float(s: &mut Sym<'_>) -> f64 {
    match take(s) {
        Sym::Float(f) => f,
        _ => 0.0,
    }
}

fn uint(s: &mut Sym<'_>) -> u64 {
    match take(s) {
        Sym::Uint(u) => u,
        _ => 0,
    }
}

fn number_literal(e: Expr) -> Box<NumberLiteral> {
    match e {
        Expr::NumberLiteral(n) => n,
        _ => Box::default(),
    }
}

fn bin_mod(s: &mut Sym<'_>) -> Box<BinMod> {
    match node(s) {
        Some(Node::BinMod(b)) => b,
        _ => Box::default(),
    }
}

fn series(s: &mut Sym<'_>) -> Vec<SequenceValue> {
    match take(s) {
        Sym::Series(v) => v,
        _ => Vec::new(),
    }
}

fn histogram(s: &mut Sym<'_>) -> Box<FloatHistogram> {
    match take(s) {
        Sym::Histogram(h) => h,
        _ => Box::default(),
    }
}

fn descriptors(s: &mut Sym<'_>) -> HistDesc {
    match take(s) {
        Sym::Descriptors(d) => d,
        _ => HistDesc::default(),
    }
}

fn bucket_set(s: &mut Sym<'_>) -> Vec<f64> {
    match take(s) {
        Sym::BucketSet(v) => v,
        _ => Vec::new(),
    }
}

fn number_lit(val: f64, pos_range: PositionRange) -> Expr {
    Expr::NumberLiteral(Box::new(NumberLiteral { val, duration: false, pos_range }))
}

fn zero_literal() -> Expr {
    number_lit(0.0, PositionRange::default())
}

/// Go `durationLiteralOutOfRange`: whether `val` seconds overflow a `time.Duration`.
fn duration_literal_out_of_range(val: f64) -> bool {
    const LIMIT: f64 = (1u64 << 63) as f64 / 1e9;
    val > LIMIT || val < -LIMIT
}

fn item_range(a: &Item<'_>, b: &Item<'_>) -> PositionRange {
    PositionRange { start: a.pos, end: b.position_range().end }
}

/// The parser state (Go `parser` plus `yyParserImpl`).
pub(crate) struct YyParser<'a, 'o> {
    pub(crate) lex: Lexer<'a>,
    inject: i32,
    injecting: bool,
    functions: &'o FunctionMap,
    /// The end position of the last item that could end an expression.
    last_closing: Pos,
    /// The last item from the lexer (Go `yyParser.lval.item`).
    lval: Item<'a>,
    /// The lookahead token (Go `yyParser.char`).
    char: i32,
    result: Option<ParseResult>,
    pub(crate) parse_errors: Vec<ParseErr>,
    last_histogram_counter_reset_hint_set: bool,
    pub(crate) options: &'o Options,
}

impl<'a, 'o> YyParser<'a, 'o> {
    pub(crate) fn new(input: &'a str, options: &'o Options, series_desc: bool) -> Self {
        YyParser {
            lex: Lexer::new(input)
                .with_unquoted_utf8_names(options.enable_unquoted_utf8_names)
                .with_series_desc(series_desc),
            inject: 0,
            injecting: false,
            functions: options.functions(),
            last_closing: 0,
            lval: Item::default(),
            char: -1,
            result: None,
            parse_errors: Vec::new(),
            last_histogram_counter_reset_hint_set: false,
            options,
        }
    }

    /// Runs the generated parser with the start symbol injected into the token stream.
    pub(crate) fn parse_generated(&mut self, start: Start) -> Option<ParseResult> {
        let t = match start {
            Start::Metric => START_METRIC,
            Start::SeriesDescription => START_SERIES_DESCRIPTION,
            Start::Expression => START_EXPRESSION,
            Start::MetricSelector => START_METRIC_SELECTOR,
        };
        self.inject_item(t.0 as i32);
        self.yy_parse();
        self.result.take()
    }

    fn inject_item(&mut self, typ: i32) {
        if self.injecting {
            panic!("cannot inject multiple Items into the token stream");
        }
        if typ != 0 && (typ <= startSymbolsStart.0 as i32 || typ >= startSymbolsEnd.0 as i32) {
            panic!("cannot inject symbol that isn't start symbol");
        }
        self.inject = typ;
        self.injecting = true;
    }

    /// The input string.
    pub(crate) fn input(&self) -> &'a str {
        self.lex.input
    }

    pub(crate) fn add_parse_err(&mut self, position_range: PositionRange, err: String) {
        self.parse_errors.push(ParseErr { position_range, err, query: self.lex.input.to_string(), line_offset: 0 });
    }

    /// An error at the position of the last lexed item.
    fn add_err_at_lval(&mut self, err: String) {
        let pr = self.lval.position_range();
        self.add_parse_err(pr, err);
    }

    /// A parser error complaining about the last item produced by the lexer.
    fn unexpected(&mut self, context: &str, expected: &str) {
        // Do not report lexer errors twice.
        if self.lval.typ == ERROR {
            return;
        }
        let mut msg = String::from("unexpected ");
        msg.push_str(&self.lval.desc());
        if !context.is_empty() {
            msg.push_str(" in ");
            msg.push_str(context);
        }
        if !expected.is_empty() {
            msg.push_str(", expected ");
            msg.push_str(expected);
        }
        self.add_err_at_lval(msg);
    }

    /// The next token for the generated parser (Go `parser.Lex`). Comments are skipped.
    fn lex_token(&mut self) -> i32 {
        if self.injecting {
            self.injecting = false;
            return self.inject;
        }
        let mut lval = std::mem::take(&mut self.lval);
        loop {
            self.lex.next_item(&mut lval);
            if lval.typ != COMMENT {
                break;
            }
        }
        self.lval = lval;
        let typ = self.lval.typ;
        if typ == ERROR {
            let pos = PositionRange { start: self.lex.start as Pos, end: self.lex.input.len() as Pos };
            let msg = self.lval.val.to_string();
            self.add_parse_err(pos, msg);
            // Tells yacc that this is the end of input.
            return 0;
        } else if typ == EOF {
            self.inject_item(0);
        } else if matches!(typ, RIGHT_BRACE | RIGHT_PAREN | RIGHT_BRACKET | DURATION | NUMBER) {
            self.last_closing = self.lval.pos + self.lval.val.len() as Pos;
        }
        typ.0 as i32
    }

    /// Go `yylex1`: the next token in the internal numbering.
    fn yylex1(&mut self) -> (i32, i32) {
        let ch = self.lex_token();
        let mut token = 0;
        if ch <= 0 {
            token = YY_TOK1[0] as i32;
        } else if (ch as usize) < YY_TOK1.len() {
            token = YY_TOK1[ch as usize] as i32;
        } else if ch >= YY_PRIVATE && ch < YY_PRIVATE + YY_TOK2.len() as i32 {
            token = YY_TOK2[(ch - YY_PRIVATE) as usize] as i32;
        } else {
            let mut i = 0;
            while i + 1 < YY_TOK3.len() {
                if YY_TOK3[i] as i32 == ch {
                    token = YY_TOK3[i + 1] as i32;
                    break;
                }
                i += 2;
            }
        }
        if token == 0 {
            token = YY_TOK2[1] as i32; // Unknown char.
        }
        (ch, token)
    }

    /// The goyacc driver (Go `yyParserImpl.Parse`). Returns 0 on accept, 1 on abort.
    fn yy_parse(&mut self) -> i32 {
        let mut stack: Vec<(i32, Sym<'a>)> = Vec::with_capacity(16);
        let mut yyval: Sym<'a> = Sym::None;
        let mut errflag = 0;
        let mut yystate: i32 = 0;
        self.char = -1;
        let mut yytoken: i32 = -1;

        'stack: loop {
            // Put a state and value onto the stack.
            stack.push((yystate, std::mem::take(&mut yyval)));

            'newstate: loop {
                let mut yyn = YY_PACT[yystate as usize] as i32;
                'shift: {
                    if yyn <= YY_FLAG {
                        break 'shift; // Simple state.
                    }
                    if self.char < 0 {
                        (self.char, yytoken) = self.yylex1();
                    }
                    yyn += yytoken;
                    if !(0..YY_LAST).contains(&yyn) {
                        break 'shift;
                    }
                    yyn = YY_ACT[yyn as usize] as i32;
                    if YY_CHK[yyn as usize] as i32 == yytoken {
                        // Valid shift.
                        self.char = -1;
                        yytoken = -1;
                        yyval = Sym::Item(self.lval.clone());
                        yystate = yyn;
                        if errflag > 0 {
                            errflag -= 1;
                        }
                        continue 'stack;
                    }
                }

                // Default state action.
                yyn = YY_DEF[yystate as usize] as i32;
                if yyn == -2 {
                    if self.char < 0 {
                        (self.char, yytoken) = self.yylex1();
                    }
                    // Look through the exception table.
                    let mut xi = 0;
                    while !(YY_EXCA[xi] == -1 && YY_EXCA[xi + 1] as i32 == yystate) {
                        xi += 2;
                    }
                    xi += 2;
                    loop {
                        yyn = YY_EXCA[xi] as i32;
                        if yyn < 0 || yyn == yytoken {
                            break;
                        }
                        xi += 2;
                    }
                    yyn = YY_EXCA[xi + 1] as i32;
                    if yyn < 0 {
                        return 0;
                    }
                }
                if yyn == 0 {
                    // Error: attempt to resume parsing.
                    if errflag == 3 {
                        // No shift yet; clobber the input char.
                        if yytoken == YY_EOF_CODE {
                            return 1;
                        }
                        self.char = -1;
                        yytoken = -1;
                        continue 'newstate;
                    }
                    errflag = 3;
                    // Find a state where "error" is a legal shift action.
                    while let Some(&(s, _)) = stack.last() {
                        let n = YY_PACT[s as usize] as i32 + YY_ERR_CODE;
                        if (0..YY_LAST).contains(&n) {
                            yystate = YY_ACT[n as usize] as i32; // Simulate a shift of "error".
                            if YY_CHK[yystate as usize] as i32 == YY_ERR_CODE {
                                continue 'stack;
                            }
                        }
                        // The current state has no shift on "error": pop the stack.
                        stack.pop();
                    }
                    // There is no state on the stack with an error shift: abort.
                    return 1;
                }

                // Reduction by production yyn.
                let rule = yyn as usize;
                let len = YY_R2[rule] as usize;
                let base = stack.len() - len; // Index of $1; stack[base - 1] is $0.
                // Consult the goto table to find the next state.
                let lhs = YY_R1[rule] as i32;
                let yyg = YY_PGO[lhs as usize] as i32;
                let yyj = yyg + stack[base - 1].0 + 1;
                if yyj >= YY_LAST {
                    yystate = YY_ACT[yyg as usize] as i32;
                } else {
                    yystate = YY_ACT[yyj as usize] as i32;
                    if YY_CHK[yystate as usize] as i32 != -lhs {
                        yystate = YY_ACT[yyg as usize] as i32;
                    }
                }
                // Move the values of the rule into a fixed array: no allocation per reduction (the longest
                // rule has 7 symbols).
                let mut args: [Sym<'a>; 8] = Default::default();
                for (i, (_, v)) in stack.drain(base..).enumerate() {
                    args[i] = v;
                }
                yyval = self.reduce(rule, &mut args[..len]);
                continue 'stack;
            }
        }
    }

    /// The semantic action of a rule. `v[0]` is `$1`. Rules without an action return `$1` (Go copies `$1`
    /// into `$$` before every action).
    fn reduce(&mut self, rule: usize, v: &mut [Sym<'a>]) -> Sym<'a> {
        match rule {
            // start: START_METRIC metric
            1 => {
                if let Sym::Labels(l) = take(&mut v[1]) {
                    self.result = Some(ParseResult::Labels(l));
                }
                Sym::None
            }
            // start: START_EXPRESSION EOF
            3 => {
                self.add_parse_err(PositionRange::default(), "no expression found in input".into());
                Sym::None
            }
            // start: START_EXPRESSION expr | START_METRIC_SELECTOR vector_selector
            4 | 5 => {
                let e = expr(&mut v[1]);
                self.result = Some(ParseResult::Expr(e));
                Sym::None
            }
            // start: error
            7 => {
                self.unexpected("", "");
                Sym::None
            }
            // aggregate_expr: aggregate_op aggregate_modifier function_call_body
            23 => {
                let op = item(&mut v[0]);
                let m = node(&mut v[1]);
                let args = exprs(&mut v[2]);
                self.new_aggregate_expr(&op, m, args, false)
            }
            // aggregate_expr: aggregate_op function_call_body aggregate_modifier
            24 => {
                let op = item(&mut v[0]);
                let args = exprs(&mut v[1]);
                let m = node(&mut v[2]);
                self.new_aggregate_expr(&op, m, args, false)
            }
            // aggregate_expr: aggregate_op function_call_body
            25 => {
                let op = item(&mut v[0]);
                let args = exprs(&mut v[1]);
                self.new_aggregate_expr(&op, None, args, true)
            }
            // aggregate_expr: aggregate_op error
            26 => {
                self.unexpected("aggregation", "");
                let op = item(&mut v[0]);
                self.new_aggregate_expr(&op, None, Vec::new(), false)
            }
            // aggregate_modifier: BY grouping_labels | WITHOUT grouping_labels
            27 | 28 => {
                let grouping = strings(&mut v[1]);
                Sym::Node(Node::AggMod(Box::new(AggregateExpr { grouping, without: rule == 28, ..Default::default() })))
            }
            // binary_expr: expr <op> bin_modifier expr
            29..=46 => {
                let lhs = expr(&mut v[0]);
                let op = item(&mut v[1]);
                let m = bin_mod(&mut v[2]);
                let rhs = expr(&mut v[3]);
                self.new_binary_expression(lhs, &op, *m, rhs)
            }
            // bool_modifier: /* empty */ | BOOL
            48 | 49 => Sym::Node(Node::BinMod(Box::new(BinMod {
                vector_matching: VectorMatching { card: VectorMatchCardinality::OneToOne, ..Default::default() },
                return_bool: rule == 49,
            }))),
            // on_or_ignoring: bool_modifier IGNORING grouping_labels | bool_modifier ON grouping_labels
            50 | 51 => {
                let mut m = bin_mod(&mut v[0]);
                m.vector_matching.matching_labels = strings(&mut v[2]);
                if rule == 51 {
                    m.vector_matching.on = true;
                }
                Sym::Node(Node::BinMod(m))
            }
            // group_modifiers: on_or_ignoring GROUP_LEFT/GROUP_RIGHT maybe_grouping_labels
            54 | 55 => {
                let mut m = bin_mod(&mut v[0]);
                m.vector_matching.card =
                    if rule == 54 { VectorMatchCardinality::ManyToOne } else { VectorMatchCardinality::OneToMany };
                m.vector_matching.include = strings(&mut v[2]);
                Sym::Node(Node::BinMod(m))
            }
            // fill_modifiers: group_modifiers FILL fill_value | FILL_LEFT | FILL_RIGHT
            57..=59 => {
                let mut m = bin_mod(&mut v[0]);
                let fill = number_literal(expr(&mut v[2])).val;
                let fv = &mut m.vector_matching.fill_values;
                match rule {
                    57 => {
                        fv.lhs = Some(fill);
                        fv.rhs = Some(fill);
                    }
                    58 => fv.lhs = Some(fill),
                    _ => fv.rhs = Some(fill),
                }
                Sym::Node(Node::BinMod(m))
            }
            // fill_modifiers: group_modifiers FILL_LEFT fill_value FILL_RIGHT fill_value (and the reverse)
            60 | 61 => {
                let mut m = bin_mod(&mut v[0]);
                let first = number_literal(expr(&mut v[2])).val;
                let second = number_literal(expr(&mut v[4])).val;
                let fv = &mut m.vector_matching.fill_values;
                if rule == 60 {
                    fv.lhs = Some(first);
                    fv.rhs = Some(second);
                } else {
                    fv.rhs = Some(first);
                    fv.lhs = Some(second);
                }
                Sym::Node(Node::BinMod(m))
            }
            // grouping_labels: LEFT_PAREN grouping_label_list RIGHT_PAREN | ... COMMA RIGHT_PAREN
            62 | 63 => take(&mut v[1]),
            // grouping_labels: LEFT_PAREN RIGHT_PAREN
            64 => Sym::Strings(Vec::new()),
            // grouping_labels: error
            65 => {
                self.unexpected("grouping opts", "\"(\"");
                Sym::Strings(Vec::new())
            }
            // grouping_label_list: grouping_label_list COMMA grouping_label
            66 => {
                let mut list = strings(&mut v[0]);
                list.push(item(&mut v[2]).val.into_owned());
                Sym::Strings(list)
            }
            // grouping_label_list: grouping_label
            67 => Sym::Strings(vec![item(&mut v[0]).val.into_owned()]),
            // grouping_label_list: grouping_label_list error
            68 => {
                self.unexpected("grouping opts", "\",\" or \")\"");
                take(&mut v[0])
            }
            // grouping_label: maybe_label
            69 => {
                let it = item(&mut v[0]);
                // Preserve legacy acceptance of unquoted colon-containing labels when the feature is disabled.
                if it.val.is_empty()
                    || (self.options.enable_unquoted_utf8_names
                        && super::lex::label_name_requires_quoting(&it.val, true))
                {
                    self.add_parse_err(
                        it.position_range(),
                        format!("invalid label name for grouping: {}", go_quote(&it.val)),
                    );
                }
                Sym::Item(it)
            }
            // grouping_label: STRING
            70 => {
                let mut it = item(&mut v[0]);
                let unquoted = self.unquote_string(&it.val);
                if unquoted.is_empty() {
                    self.add_parse_err(
                        it.position_range(),
                        format!("invalid label name for grouping: {}", go_quote(&unquoted)),
                    );
                }
                it.pos += 1;
                it.val = Cow::Owned(unquoted);
                Sym::Item(it)
            }
            // grouping_label: error
            71 => {
                self.unexpected("grouping opts", "label");
                Sym::Item(Item::default())
            }
            // fill_value: LEFT_PAREN number_duration_literal RIGHT_PAREN
            72 => take(&mut v[1]),
            // fill_value: LEFT_PAREN unary_op number_duration_literal RIGHT_PAREN
            73 => {
                let op = item(&mut v[1]);
                let mut nl = number_literal(expr(&mut v[2]));
                if op.typ == SUB {
                    nl.val *= -1.0;
                }
                nl.pos_range.start = op.pos;
                Sym::Node(Node::Expr(Expr::NumberLiteral(nl)))
            }
            // function_call: IDENTIFIER | at_modifier_preprocessors | STEP | RANGE | max_of_min_of,
            // followed by function_call_body
            74..=78 => {
                let name = item(&mut v[0]);
                let args = exprs(&mut v[1]);
                let func = match get_function(&name.val, self.functions) {
                    Some(f) => f.clone(),
                    None => {
                        self.add_parse_err(
                            name.position_range(),
                            format!("unknown function with name {}", go_quote(&name.val)),
                        );
                        // Keep the name so the partially-built AST stays printable; the recorded error still
                        // rejects the query.
                        Arc::new(Function::new(&name.val, Vec::new(), 0, ValueType::None, false))
                    }
                };
                if func.experimental && !self.options.enable_experimental_functions {
                    self.add_parse_err(
                        name.position_range(),
                        format!("function {} is not enabled", go_quote(&name.val)),
                    );
                }
                Sym::Node(Node::Expr(Expr::Call(Box::new(Call {
                    func,
                    args,
                    pos_range: PositionRange { start: name.pos, end: self.last_closing },
                }))))
            }
            // function_call_body: LEFT_PAREN function_call_args RIGHT_PAREN
            79 => take(&mut v[1]),
            // function_call_body: LEFT_PAREN RIGHT_PAREN
            80 => Sym::Node(Node::Exprs(Vec::new())),
            // function_call_args: function_call_args COMMA expr
            81 => {
                let mut list = exprs(&mut v[0]);
                list.push(expr(&mut v[2]));
                Sym::Node(Node::Exprs(list))
            }
            // function_call_args: expr
            82 => Sym::Node(Node::Exprs(vec![expr(&mut v[0])])),
            // function_call_args: function_call_args COMMA
            83 => {
                let comma = item(&mut v[1]);
                self.add_parse_err(comma.position_range(), "trailing commas not allowed in function call args".into());
                take(&mut v[0])
            }
            // paren_expr: LEFT_PAREN expr RIGHT_PAREN
            84 => {
                let l = item(&mut v[0]);
                let e = expr(&mut v[1]);
                let r = item(&mut v[2]);
                Sym::Node(Node::Expr(Expr::Paren(Box::new(ParenExpr { expr: e, pos_range: item_range(&l, &r) }))))
            }
            // positive_duration_expr: duration_expr
            85 => {
                let e = expr(&mut v[0]);
                if let Expr::NumberLiteral(nl) = &e {
                    if nl.val <= 0.0 {
                        self.add_parse_err(nl.pos_range, "duration must be greater than 0".into());
                        return Sym::Node(Node::Expr(zero_literal()));
                    }
                }
                Sym::Node(Node::Expr(e))
            }
            // offset_expr: expr OFFSET offset_duration_expr
            86 => {
                let mut e = expr(&mut v[0]);
                match expr(&mut v[2]) {
                    Expr::NumberLiteral(nl) => self.add_offset(&mut e, seconds_to_duration(nl.val)),
                    Expr::Duration(de) => self.add_offset_expr(&mut e, de),
                    _ => {}
                }
                Sym::Node(Node::Expr(e))
            }
            // offset_expr: expr OFFSET error
            87 => {
                self.unexpected("offset", "number, duration, step(), or range()");
                take(&mut v[0])
            }
            // anchored_expr: expr ANCHORED
            88 => {
                let mut e = expr(&mut v[0]);
                self.set_anchored(&mut e);
                Sym::Node(Node::Expr(e))
            }
            // smoothed_expr: expr SMOOTHED
            89 => {
                let mut e = expr(&mut v[0]);
                self.set_smoothed(&mut e);
                Sym::Node(Node::Expr(e))
            }
            // step_invariant_expr: expr AT signed_or_unsigned_number
            90 => {
                let mut e = expr(&mut v[0]);
                let ts = float(&mut v[2]);
                self.set_timestamp(&mut e, ts);
                Sym::Node(Node::Expr(e))
            }
            // step_invariant_expr: expr AT at_modifier_preprocessors LEFT_PAREN RIGHT_PAREN
            91 => {
                let mut e = expr(&mut v[0]);
                let op = item(&mut v[2]);
                self.set_at_modifier_preprocessor(&mut e, &op);
                Sym::Node(Node::Expr(e))
            }
            // step_invariant_expr: expr AT error
            92 => {
                self.unexpected("@", "timestamp");
                take(&mut v[0])
            }
            // matrix_selector: expr LEFT_BRACKET positive_duration_expr RIGHT_BRACKET
            95 => {
                let e = expr(&mut v[0]);
                let lb = item(&mut v[1]);
                let d = expr(&mut v[2]);
                let rb = item(&mut v[3]);
                let err_msg = match &e {
                    Expr::VectorSelector(vs) => {
                        if vs.original_offset != 0 || vs.original_offset_expr.is_some() {
                            "no offset modifiers allowed before range"
                        } else if vs.timestamp.is_some() || vs.start_or_end != ItemType(0) {
                            "no @ modifiers allowed before range"
                        } else {
                            ""
                        }
                    }
                    _ => "ranges only allowed for vector selectors",
                };
                if !err_msg.is_empty() {
                    self.add_parse_err(item_range(&lb, &rb), err_msg.to_string());
                }
                let (range, range_expr) = split_duration(d);
                Sym::Node(Node::Expr(Expr::MatrixSelector(Box::new(MatrixSelector {
                    vector_selector: e,
                    range,
                    range_expr,
                    end_pos: self.last_closing,
                }))))
            }
            // subquery_expr: expr LEFT_BRACKET positive_duration_expr COLON positive_duration_expr RIGHT_BRACKET
            96 => {
                let e = expr(&mut v[0]);
                let (range, range_expr) = split_duration(expr(&mut v[2]));
                let (step, step_expr) = split_duration(expr(&mut v[4]));
                let rb = item(&mut v[5]);
                Sym::Node(Node::Expr(Expr::Subquery(Box::new(SubqueryExpr {
                    expr: e,
                    range,
                    range_expr,
                    original_offset: 0,
                    original_offset_expr: None,
                    offset: 0,
                    timestamp: None,
                    start_or_end: ItemType(0),
                    step,
                    step_expr,
                    end_pos: rb.pos + 1,
                }))))
            }
            // subquery_expr: expr LEFT_BRACKET positive_duration_expr COLON RIGHT_BRACKET
            97 => {
                let e = expr(&mut v[0]);
                let (range, range_expr) = split_duration(expr(&mut v[2]));
                let rb = item(&mut v[4]);
                Sym::Node(Node::Expr(Expr::Subquery(Box::new(SubqueryExpr {
                    expr: e,
                    range,
                    range_expr,
                    original_offset: 0,
                    original_offset_expr: None,
                    offset: 0,
                    timestamp: None,
                    start_or_end: ItemType(0),
                    step: 0,
                    step_expr: None,
                    end_pos: rb.pos + 1,
                }))))
            }
            98 => {
                self.unexpected("subquery selector", "\"]\"");
                take(&mut v[0])
            }
            99 => {
                self.unexpected("subquery selector", "number, duration, step(), range(), or \"]\"");
                take(&mut v[0])
            }
            100 => {
                self.unexpected("subquery or range", "\":\" or \"]\"");
                take(&mut v[0])
            }
            101 => {
                self.unexpected("subquery or range selector", "number, duration, step(), or range()");
                take(&mut v[0])
            }
            // unary_expr: unary_op expr
            102 => {
                let op = item(&mut v[0]);
                let e = expr(&mut v[1]);
                match e {
                    Expr::NumberLiteral(mut nl) => {
                        if op.typ == SUB {
                            nl.val *= -1.0;
                        }
                        nl.pos_range.start = op.pos;
                        Sym::Node(Node::Expr(Expr::NumberLiteral(nl)))
                    }
                    e => Sym::Node(Node::Expr(Expr::Unary(Box::new(UnaryExpr {
                        op: op.typ,
                        expr: e,
                        start_pos: op.pos,
                    })))),
                }
            }
            // vector_selector: metric_identifier label_matchers
            103 => {
                let id = item(&mut v[0]);
                let mut e = expr(&mut v[1]);
                if let Expr::VectorSelector(vs) = &mut e {
                    vs.pos_range.start = id.pos;
                    vs.name = id.val.into_owned();
                    self.assemble_vector_selector(vs);
                }
                Sym::Node(Node::Expr(e))
            }
            // vector_selector: metric_identifier
            104 => {
                let id = item(&mut v[0]);
                let mut vs = Box::new(VectorSelector {
                    pos_range: id.position_range(),
                    name: id.val.into_owned(),
                    ..Default::default()
                });
                self.assemble_vector_selector(&mut vs);
                Sym::Node(Node::Expr(Expr::VectorSelector(vs)))
            }
            // vector_selector: label_matchers
            105 => {
                let mut e = expr(&mut v[0]);
                if let Expr::VectorSelector(vs) = &mut e {
                    self.assemble_vector_selector(vs);
                }
                Sym::Node(Node::Expr(e))
            }
            // label_matchers: LEFT_BRACE label_match_list RIGHT_BRACE | ... COMMA RIGHT_BRACE | LEFT_BRACE RIGHT_BRACE
            106..=108 => {
                let l = item(&mut v[0]);
                let r = item(&mut v[v.len() - 1]);
                let label_matchers = match take(&mut v[1]) {
                    Sym::Matchers(m) if rule != 108 => m,
                    _ => Vec::new(),
                };
                Sym::Node(Node::Expr(Expr::VectorSelector(Box::new(VectorSelector {
                    label_matchers,
                    pos_range: item_range(&l, &r),
                    ..Default::default()
                }))))
            }
            // label_match_list: label_match_list COMMA label_matcher
            109 => {
                let mut list = match take(&mut v[0]) {
                    Sym::Matchers(m) => m,
                    _ => Vec::new(),
                };
                // A nil matcher failed to build (invalid regexp or incomplete syntax) and its error is already
                // recorded. Drop it so the partially-built AST never holds a nil matcher.
                if let Sym::Matcher(Some(m)) = take(&mut v[2]) {
                    list.push(m);
                }
                Sym::Matchers(list)
            }
            // label_match_list: label_matcher
            110 => match take(&mut v[0]) {
                Sym::Matcher(Some(m)) => Sym::Matchers(vec![m]),
                _ => Sym::Matchers(Vec::new()),
            },
            // label_match_list: label_match_list error
            111 => {
                self.unexpected("label matching", "\",\" or \"}\"");
                take(&mut v[0])
            }
            // label_matcher: IDENTIFIER match_op STRING | string_identifier match_op STRING
            112 | 113 => {
                let label = item(&mut v[0]);
                let op = item(&mut v[1]);
                let value = item(&mut v[2]);
                Sym::Matcher(self.new_label_matcher(&label, &op, &value))
            }
            // label_matcher: string_identifier
            114 => {
                let value = item(&mut v[0]);
                Sym::Matcher(Some(Matcher::must_new(MatchType::Equal, METRIC_NAME, &value.val)))
            }
            115 | 116 => {
                self.unexpected("label matching", "string");
                Sym::Matcher(None)
            }
            117 => {
                self.unexpected("label matching", "label matching operator");
                Sym::Matcher(None)
            }
            118 => {
                self.unexpected("label matching", "identifier or \"}\"");
                Sym::Matcher(None)
            }
            // metric: metric_identifier label_set
            119 => {
                let id = item(&mut v[0]);
                let ls = match take(&mut v[1]) {
                    Sym::Labels(l) => l,
                    _ => Labels::empty(),
                };
                let mut b = labels::Builder::new(ls);
                b.set(METRIC_NAME, &id.val);
                Sym::Labels(b.labels())
            }
            // label_set: LEFT_BRACE label_set_list RIGHT_BRACE | ... COMMA RIGHT_BRACE
            154 | 155 => match take(&mut v[1]) {
                Sym::LblList(mut l) => Sym::Labels(labels::new(&mut l)),
                _ => Sym::Labels(Labels::empty()),
            },
            // label_set: LEFT_BRACE RIGHT_BRACE | /* empty */
            156 | 157 => Sym::Labels(Labels::empty()),
            // label_set_list: label_set_list COMMA label_set_item
            158 => {
                let mut list = match take(&mut v[0]) {
                    Sym::LblList(l) => l,
                    _ => Vec::new(),
                };
                if let Sym::Label(l) = take(&mut v[2]) {
                    list.push(l);
                }
                Sym::LblList(list)
            }
            // label_set_list: label_set_item
            159 => match take(&mut v[0]) {
                Sym::Label(l) => Sym::LblList(vec![l]),
                _ => Sym::LblList(Vec::new()),
            },
            160 => {
                self.unexpected("label set", "\",\" or \"}\"");
                take(&mut v[0])
            }
            // label_set_item: IDENTIFIER EQL STRING | string_identifier EQL STRING
            161 | 162 => {
                let name = item(&mut v[0]);
                let value = item(&mut v[2]);
                let value = self.unquote_string(&value.val);
                Sym::Label(Label::new(name.val.into_owned(), value))
            }
            // label_set_item: string_identifier
            163 => {
                let name = item(&mut v[0]);
                Sym::Label(Label::new(METRIC_NAME, name.val.into_owned()))
            }
            164 | 165 => {
                self.unexpected("label set", "string");
                Sym::Label(Label::default())
            }
            166 => {
                self.unexpected("label set", "\"=\"");
                Sym::Label(Label::default())
            }
            167 => {
                self.unexpected("label set", "identifier or \"}\"");
                Sym::Label(Label::default())
            }
            // series_description: metric series_values
            168 => {
                let ls = match take(&mut v[0]) {
                    Sym::Labels(l) => l,
                    _ => Labels::empty(),
                };
                let vals = series(&mut v[1]);
                self.result = Some(ParseResult::SeriesDesc(ls, vals));
                Sym::None
            }
            // series_values: /* empty */
            169 => Sym::Series(Vec::new()),
            // series_values: series_values SPACE series_item
            170 => {
                let mut list = series(&mut v[0]);
                list.append(&mut series(&mut v[2]));
                Sym::Series(list)
            }
            // series_values: series_values SPACE
            171 => take(&mut v[0]),
            // series_values: error
            172 => {
                self.unexpected("series values", "");
                Sym::Series(Vec::new())
            }
            // series_item: BLANK
            173 => Sym::Series(vec![SequenceValue { omitted: true, ..Default::default() }]),
            // series_item: BLANK TIMES uint
            174 => {
                let n = uint(&mut v[2]);
                let mut list = Vec::new();
                for _ in 0..n {
                    list.push(SequenceValue { omitted: true, ..Default::default() });
                }
                Sym::Series(list)
            }
            // series_item: series_value
            175 => Sym::Series(vec![SequenceValue { value: float(&mut v[0]), ..Default::default() }]),
            // series_item: series_value TIMES uint
            176 => {
                let val = float(&mut v[0]);
                let n = uint(&mut v[2]);
                let mut list = Vec::new();
                // Add an additional value for time 0, which we ignore in tests.
                let mut i = 0u64;
                while i <= n {
                    list.push(SequenceValue { value: val, ..Default::default() });
                    i += 1;
                }
                Sym::Series(list)
            }
            // series_item: series_value signed_number TIMES uint
            177 => {
                let mut val = float(&mut v[0]);
                let inc = float(&mut v[1]);
                let n = uint(&mut v[3]);
                let mut list = Vec::new();
                // Add an additional value for time 0, which we ignore in tests.
                let mut i = 0u64;
                while i <= n {
                    list.push(SequenceValue { value: val, ..Default::default() });
                    val += inc;
                    i += 1;
                }
                Sym::Series(list)
            }
            // series_item: histogram_series_value
            178 => {
                let h = histogram(&mut v[0]);
                Sym::Series(vec![self.new_histogram_sequence_value(h)])
            }
            // series_item: histogram_series_value TIMES uint
            179 => {
                let h = histogram(&mut v[0]);
                let n = uint(&mut v[2]);
                let sv = self.new_histogram_sequence_value(h);
                let mut list = Vec::new();
                let mut i = 0u64;
                while i <= n {
                    list.push(sv.clone());
                    i += 1;
                }
                Sym::Series(list)
            }
            // series_item: histogram_series_value ADD/SUB histogram_series_value TIMES uint
            180 | 181 => {
                let base = histogram(&mut v[0]);
                let inc = histogram(&mut v[2]);
                let n = uint(&mut v[4]);
                let (vals, err) = self.histograms_series(base, &inc, n, rule == 181);
                if let Some(err) = err {
                    self.add_err_at_lval(err);
                }
                Sym::Series(vals)
            }
            // series_value: IDENTIFIER
            182 => {
                let id = item(&mut v[0]);
                if id.val != "stale" {
                    self.unexpected("series values", "number or \"stale\"");
                }
                Sym::Float(model::value::stale_nan())
            }
            // histogram_series_value: OPEN_HIST histogram_desc_map [SPACE] CLOSE_HIST
            185 | 186 => {
                let d = descriptors(&mut v[1]);
                Sym::Histogram(self.build_histogram_from_map(&d))
            }
            // histogram_series_value: OPEN_HIST [SPACE] CLOSE_HIST
            187 | 188 => Sym::Histogram(self.build_histogram_from_map(&HistDesc::default())),
            // histogram_desc_map: histogram_desc_map SPACE histogram_desc_item
            189 => {
                let mut left = descriptors(&mut v[0]);
                let right = descriptors(&mut v[2]);
                for (k, val) in right.0 {
                    if left.get(k).is_some() {
                        self.add_parse_err(PositionRange::default(), format!("duplicate key \"{}\" in histogram", k));
                        continue;
                    }
                    left.0.push((k, val));
                }
                Sym::Descriptors(left)
            }
            // histogram_desc_map: histogram_desc_map error
            191 => {
                self.unexpected("histogram description", "histogram description key, e.g. buckets:[5 10 7]");
                take(&mut v[0])
            }
            // histogram_desc_item: <KEY>_DESC COLON value
            192..=202 => {
                let key = match rule {
                    192 => "schema",
                    193 => "sum",
                    194 => "count",
                    195 => "z_bucket",
                    196 => "z_bucket_w",
                    197 => "custom_values",
                    198 => "buckets",
                    199 => "offset",
                    200 => "n_buckets",
                    201 => "n_offset",
                    _ => "counter_reset_hint",
                };
                let val = match take(&mut v[2]) {
                    Sym::Int(i) => DescVal::Int(i),
                    Sym::Float(f) => DescVal::Float(f),
                    Sym::BucketSet(b) => DescVal::Floats(b),
                    Sym::Item(it) => DescVal::Hint(it.typ),
                    _ => DescVal::Int(0),
                };
                Sym::Descriptors(HistDesc::one(key, val))
            }
            // bucket_set: LEFT_BRACKET bucket_set_list [SPACE] RIGHT_BRACKET
            203 | 204 => take(&mut v[1]),
            // bucket_set_list: bucket_set_list SPACE signed_or_unsigned_number
            205 => {
                let mut list = bucket_set(&mut v[0]);
                list.push(float(&mut v[2]));
                Sym::BucketSet(list)
            }
            // bucket_set_list: signed_or_unsigned_number
            206 => Sym::BucketSet(vec![float(&mut v[0])]),
            // number_duration_literal: NUMBER
            270 => {
                let it = item(&mut v[0]);
                let val = self.number(&it.val);
                Sym::Node(Node::Expr(number_lit(val, it.position_range())))
            }
            // number_duration_literal: DURATION
            271 => {
                let it = item(&mut v[0]);
                let dur = match parse_duration(&it.val) {
                    Ok(d) => d,
                    Err(e) => {
                        self.add_parse_err(it.position_range(), e);
                        0
                    }
                };
                Sym::Node(Node::Expr(Expr::NumberLiteral(Box::new(NumberLiteral {
                    val: duration_seconds(dur),
                    duration: true,
                    pos_range: it.position_range(),
                }))))
            }
            // number: NUMBER
            272 => {
                let it = item(&mut v[0]);
                Sym::Float(self.number(&it.val))
            }
            // number: DURATION
            273 => {
                let it = item(&mut v[0]);
                let dur = match parse_duration(&it.val) {
                    Ok(d) => d,
                    Err(e) => {
                        self.add_parse_err(it.position_range(), e);
                        0
                    }
                };
                Sym::Float(duration_seconds(dur))
            }
            // signed_number: ADD number | SUB number
            274 => Sym::Float(float(&mut v[1])),
            275 => Sym::Float(-float(&mut v[1])),
            // uint: NUMBER
            278 => {
                let it = item(&mut v[0]);
                match parse_uint(&it.val, false) {
                    Ok(u) => Sym::Uint(u),
                    Err((u, e)) => {
                        self.add_parse_err(it.position_range(), format!("invalid repetition in series values: {}", e));
                        Sym::Uint(u)
                    }
                }
            }
            // int: SUB uint | uint
            279 => Sym::Int((uint(&mut v[1]) as i64).wrapping_neg()),
            280 => Sym::Int(uint(&mut v[0]) as i64),
            // string_literal: STRING
            281 => {
                let it = item(&mut v[0]);
                let invalid_utf8 = super::quote::unquote_invalid_bytes(&it.val);
                let val = self.unquote_string(&it.val);
                Sym::Node(Node::Expr(Expr::StringLiteral(Box::new(StringLiteral {
                    val,
                    invalid_utf8,
                    pos_range: it.position_range(),
                }))))
            }
            // string_identifier: STRING
            282 => {
                let it = item(&mut v[0]);
                let val = self.unquote_string(&it.val);
                Sym::Item(Item { typ: METRIC_IDENTIFIER, pos: it.pos, val: Cow::Owned(val) })
            }
            // maybe_grouping_labels: /* empty */
            283 => Sym::Strings(Vec::new()),
            // offset_duration_expr: number_duration_literal | duration_expr: number_duration_literal
            285 | 297 => {
                let nl = number_literal(expr(&mut v[0]));
                if duration_literal_out_of_range(nl.val) {
                    self.add_parse_err(nl.pos_range, "duration out of range".into());
                    return Sym::Node(Node::Expr(zero_literal()));
                }
                Sym::Node(Node::Expr(Expr::NumberLiteral(nl)))
            }
            // offset_duration_expr: unary_op number_duration_literal
            286 => {
                let op = item(&mut v[0]);
                let mut nl = number_literal(expr(&mut v[1]));
                if op.typ == SUB {
                    nl.val *= -1.0;
                }
                if duration_literal_out_of_range(nl.val) {
                    self.add_parse_err(op.position_range(), "duration out of range".into());
                    return Sym::Node(Node::Expr(zero_literal()));
                }
                nl.pos_range.start = op.pos;
                Sym::Node(Node::Expr(Expr::NumberLiteral(nl)))
            }
            // offset_duration_expr: STEP/RANGE LEFT_PAREN RIGHT_PAREN; duration_expr: the same
            287 | 288 | 305 | 306 => {
                let kw = item(&mut v[0]);
                let r = item(&mut v[2]);
                Sym::Node(Node::Expr(Expr::Duration(Box::new(DurationExpr {
                    op: kw.typ,
                    start_pos: kw.pos,
                    end_pos: r.position_range().end,
                    ..Default::default()
                }))))
            }
            // offset_duration_expr: unary_op STEP/RANGE LEFT_PAREN RIGHT_PAREN
            289 | 290 => {
                let op = item(&mut v[0]);
                let kw = item(&mut v[1]);
                let r = item(&mut v[3]);
                let inner = DurationExpr {
                    op: kw.typ,
                    start_pos: kw.pos,
                    end_pos: r.position_range().end,
                    ..Default::default()
                };
                Sym::Node(Node::Expr(Expr::Duration(Box::new(DurationExpr {
                    op: op.typ,
                    rhs: Some(Expr::Duration(Box::new(inner))),
                    start_pos: op.pos,
                    ..Default::default()
                }))))
            }
            // max_of_min_of LEFT_PAREN duration_expr COMMA duration_expr RIGHT_PAREN
            291 | 307 => {
                let kw = item(&mut v[0]);
                let lhs = expr(&mut v[2]);
                let rhs = expr(&mut v[4]);
                let r = item(&mut v[5]);
                Sym::Node(Node::Expr(Expr::Duration(Box::new(DurationExpr {
                    op: kw.typ,
                    start_pos: kw.pos,
                    end_pos: r.position_range().end,
                    lhs: Some(lhs),
                    rhs: Some(rhs),
                    wrapped: false,
                }))))
            }
            // offset_duration_expr: unary_op max_of_min_of LEFT_PAREN duration_expr COMMA duration_expr RIGHT_PAREN
            292 => {
                let op = item(&mut v[0]);
                let kw = item(&mut v[1]);
                let lhs = expr(&mut v[3]);
                let rhs = expr(&mut v[5]);
                let r = item(&mut v[6]);
                let end = r.position_range().end;
                let inner = DurationExpr {
                    op: kw.typ,
                    start_pos: kw.pos,
                    end_pos: end,
                    lhs: Some(lhs),
                    rhs: Some(rhs),
                    wrapped: false,
                };
                Sym::Node(Node::Expr(Expr::Duration(Box::new(DurationExpr {
                    op: op.typ,
                    start_pos: op.pos,
                    end_pos: end,
                    rhs: Some(Expr::Duration(Box::new(inner))),
                    ..Default::default()
                }))))
            }
            // offset_duration_expr: unary_op LEFT_PAREN duration_expr RIGHT_PAREN
            293 => {
                let op = item(&mut v[0]);
                let e = expr(&mut v[2]);
                Sym::Node(Node::Expr(self.apply_unary_op_to_duration_expr(&op, e, true)))
            }
            // duration_expr: unary_op duration_expr
            298 => {
                let op = item(&mut v[0]);
                let e = expr(&mut v[1]);
                Sym::Node(Node::Expr(self.apply_unary_op_to_duration_expr(&op, e, false)))
            }
            // duration_expr: duration_expr <op> duration_expr
            299..=304 => {
                let lhs = expr(&mut v[0]);
                let op = item(&mut v[1]);
                let rhs = expr(&mut v[2]);
                if rule == 302 || rule == 303 {
                    if let Expr::NumberLiteral(nl) = &rhs {
                        if nl.val == 0.0 {
                            let msg = if rule == 302 { "division by zero" } else { "modulo by zero" };
                            self.add_parse_err(op.position_range(), msg.into());
                            return Sym::Node(Node::Expr(zero_literal()));
                        }
                    }
                }
                Sym::Node(Node::Expr(Expr::Duration(Box::new(DurationExpr {
                    op: op.typ,
                    lhs: Some(lhs),
                    rhs: Some(rhs),
                    ..Default::default()
                }))))
            }
            // paren_duration_expr: LEFT_PAREN duration_expr RIGHT_PAREN
            309 => {
                let l = item(&mut v[0]);
                let e = expr(&mut v[1]);
                let r = item(&mut v[2]);
                Sym::Node(Node::Expr(wrap_paren_duration_expr(e, l.pos, r.position_range().end)))
            }
            // Rules without an action: $$ = $1.
            _ => {
                if v.is_empty() {
                    Sym::None
                } else {
                    take(&mut v[0])
                }
            }
        }
    }

    /// Go `newBinaryExpression`.
    fn new_binary_expression(&mut self, lhs: Expr, op: &Item<'_>, m: BinMod, rhs: Expr) -> Sym<'a> {
        let BinMod { vector_matching, return_bool } = m;
        let fill = vector_matching.fill_values;
        let ret = Expr::Binary(Box::new(BinaryExpr {
            op: op.typ,
            lhs,
            rhs,
            vector_matching: Some(Box::new(vector_matching)),
            return_bool,
            unquoted_utf8_names: self.options.enable_unquoted_utf8_names,
        }));
        if !self.options.enable_binop_fill_modifiers && (fill.lhs.is_some() || fill.rhs.is_some()) {
            self.add_parse_err(ret.position_range(), "binop fill modifiers are experimental and not enabled".into());
        }
        Sym::Node(Node::Expr(ret))
    }

    /// Go `assembleVectorSelector`.
    fn assemble_vector_selector(&self, vs: &mut VectorSelector) {
        vs.unquoted_utf8_names = self.options.enable_unquoted_utf8_names;
        // If the metric name was set outside the braces, add a matcher for it.
        if !vs.name.is_empty() {
            vs.label_matchers.push(Matcher::must_new(MatchType::Equal, METRIC_NAME, &vs.name));
        }
    }

    /// Go `newAggregateExpr`. `modifier` is the `by`/`without` clause, if any.
    fn new_aggregate_expr(
        &mut self,
        op: &Item<'_>,
        modifier: Option<Node>,
        mut args: Vec<Expr>,
        overread: bool,
    ) -> Sym<'a> {
        let mut ret = match modifier {
            Some(Node::AggMod(a)) => a,
            _ => Box::default(),
        };
        ret.unquoted_utf8_names = self.options.enable_unquoted_utf8_names;
        ret.pos_range = PositionRange { start: op.pos, end: self.last_closing };
        if overread {
            ret.pos_range.end = self.lex.find_prev_right_paren(self.last_closing);
        }
        ret.op = op.typ;

        if args.is_empty() {
            self.add_parse_err(ret.pos_range, "no arguments for aggregate expression provided".into());
            // Prevents invalid array accesses.
            return Sym::Node(Node::Expr(Expr::Aggregate(ret)));
        }

        let mut desired_args = 1;
        if ret.op.is_aggregator_with_param() {
            if !self.options.enable_experimental_functions && ret.op.is_experimental_aggregator() {
                self.add_parse_err(
                    ret.pos_range,
                    format!(
                        "{}() is experimental and must be enabled with --enable-feature=promql-experimental-functions",
                        ret.op
                    ),
                );
                return Sym::Node(Node::Expr(Expr::Aggregate(ret)));
            }
            desired_args = 2;
        }

        if args.len() != desired_args {
            self.add_parse_err(
                ret.pos_range,
                format!(
                    "wrong number of arguments for aggregate expression provided, expected {}, got {}",
                    desired_args,
                    args.len()
                ),
            );
            if desired_args == 2 {
                ret.param = Some(args.swap_remove(0));
            }
            return Sym::Node(Node::Expr(Expr::Aggregate(ret)));
        }

        ret.expr = args.pop();
        if desired_args == 2 {
            ret.param = args.pop();
        }
        Sym::Node(Node::Expr(Expr::Aggregate(ret)))
    }

    /// Go `histogramsIncreaseSeries` and `histogramsDecreaseSeries`.
    fn histograms_series(
        &self,
        base: Box<FloatHistogram>,
        inc: &FloatHistogram,
        times: u64,
        sub: bool,
    ) -> (Vec<SequenceValue>, Option<String>) {
        // Capture the hint set flag immediately after the inc histogram is built.
        let hint_set = self.last_histogram_counter_reset_hint_set;
        let mut ret = Vec::new();
        // Add an additional value (the base) for time 0, which we ignore in tests.
        ret.push(SequenceValue {
            histogram: Some(base.clone()),
            counter_reset_hint_set: hint_set,
            ..Default::default()
        });
        let mut cur = base;
        let mut i = 1u64;
        while i <= times {
            if cur.schema > inc.schema {
                return (
                    Vec::new(),
                    Some(format!(
                        "error combining histograms: cannot merge from schema {} to {}",
                        inc.schema, cur.schema
                    )),
                );
            }
            let mut next = Box::new(cur.copy());
            let res = if sub { next.sub(inc) } else { next.add(inc) };
            if let Err(e) = res {
                return (ret, Some(e.to_string()));
            }
            ret.push(SequenceValue {
                histogram: Some(next.clone()),
                counter_reset_hint_set: hint_set,
                ..Default::default()
            });
            cur = next;
            i += 1;
        }
        (ret, None)
    }

    /// Go `buildHistogramFromMap`.
    fn build_histogram_from_map(&mut self, desc: &HistDesc) -> Box<FloatHistogram> {
        let mut output = Box::<FloatHistogram>::default();
        // Reset the flag for each new histogram being built.
        self.last_histogram_counter_reset_hint_set = false;

        if let Some(v) = desc.get("schema") {
            match v {
                DescVal::Int(i) => output.schema = *i as i32,
                other => self.add_err_at_lval(format!("error parsing schema number: {}", desc_val_str(other))),
            }
        }
        for (key, field) in [("sum", 0), ("count", 1), ("z_bucket", 2), ("z_bucket_w", 3)] {
            if let Some(v) = desc.get(key) {
                match v {
                    DescVal::Float(f) => match field {
                        0 => output.sum = *f,
                        1 => output.count = *f,
                        2 => output.zero_count = *f,
                        _ => output.zero_threshold = *f,
                    },
                    other => self.add_err_at_lval(format!("error parsing {} number: {}", key, desc_val_str(other))),
                }
            }
        }
        if let Some(v) = desc.get("custom_values") {
            match v {
                DescVal::Floats(f) => output.custom_values = model::histogram::custom_values(f),
                other => self.add_err_at_lval(format!("error parsing custom_values: {}", desc_val_str(other))),
            }
        }
        if let Some(v) = desc.get("counter_reset_hint") {
            // Mark that the counter reset hint was explicitly specified.
            self.last_histogram_counter_reset_hint_set = true;
            match v {
                DescVal::Hint(t) => match *t {
                    UNKNOWN_COUNTER_RESET => output.counter_reset_hint = CounterResetHint::UnknownCounterReset,
                    COUNTER_RESET => output.counter_reset_hint = CounterResetHint::CounterReset,
                    NOT_COUNTER_RESET => output.counter_reset_hint = CounterResetHint::NotCounterReset,
                    GAUGE_TYPE => output.counter_reset_hint = CounterResetHint::GaugeType,
                    t => self.add_err_at_lval(format!("error parsing counter_reset_hint: unknown value {}", t)),
                },
                other => self.add_err_at_lval(format!("error parsing counter_reset_hint: {}", desc_val_str(other))),
            }
        }

        let (buckets, spans) = self.build_histogram_buckets_and_spans(desc, "buckets", "offset");
        output.positive_buckets = buckets;
        output.positive_spans = spans;
        let (buckets, spans) = self.build_histogram_buckets_and_spans(desc, "n_buckets", "n_offset");
        output.negative_buckets = buckets;
        output.negative_spans = spans;
        output
    }

    fn build_histogram_buckets_and_spans(
        &mut self,
        desc: &HistDesc,
        buckets_key: &str,
        offset_key: &str,
    ) -> (Vec<f64>, Vec<Span>) {
        let mut buckets = Vec::new();
        if let Some(v) = desc.get(buckets_key) {
            match v {
                DescVal::Floats(f) => buckets = f.clone(),
                other => {
                    self.add_err_at_lval(format!("error parsing {} float array: {}", buckets_key, desc_val_str(other)))
                }
            }
        }
        let mut offset = 0i32;
        if let Some(v) = desc.get(offset_key) {
            match v {
                DescVal::Int(i) => offset = *i as i32,
                other => self.add_err_at_lval(format!("error parsing {} number: {}", offset_key, desc_val_str(other))),
            }
        }
        let mut spans = Vec::new();
        if !buckets.is_empty() {
            spans.push(Span { offset, length: buckets.len() as u32 });
        }
        (buckets, spans)
    }

    /// Go `newHistogramSequenceValue`.
    fn new_histogram_sequence_value(&self, h: Box<FloatHistogram>) -> SequenceValue {
        SequenceValue {
            histogram: Some(h),
            counter_reset_hint_set: self.last_histogram_counter_reset_hint_set,
            ..Default::default()
        }
    }

    /// Go `parser.number`.
    fn number(&mut self, val: &str) -> f64 {
        if let Some(n) = parse_int_base0(val) {
            return n as f64;
        }
        match parse_float(val) {
            Ok(f) => f,
            Err((f, e)) => {
                self.add_err_at_lval(format!("error parsing number: {}", e));
                f
            }
        }
    }

    /// Go `unquoteString`.
    fn unquote_string(&mut self, s: &str) -> String {
        match unquote(s) {
            Ok(u) => u,
            Err(e) => {
                self.add_err_at_lval(format!("error unquoting string {}: {}", go_quote(s), e));
                String::new()
            }
        }
    }

    /// Go `newLabelMatcher`.
    fn new_label_matcher(&mut self, label: &Item<'_>, operator: &Item<'_>, value: &Item<'_>) -> Option<Matcher> {
        let val = self.unquote_string(&value.val);
        let match_type = match operator.typ {
            EQL => MatchType::Equal,
            NEQ => MatchType::NotEqual,
            EQL_REGEX => MatchType::Regexp,
            NEQ_REGEX => MatchType::NotRegexp,
            // This should never happen, since the error should have been caught by the generated parser.
            _ => panic!("invalid operator"),
        };
        match Matcher::new(match_type, &label.val, &val) {
            Ok(m) => Some(m),
            Err(e) => {
                self.add_parse_err(item_range(label, value), e.to_string());
                None
            }
        }
    }

    /// Go `addOffset`.
    fn add_offset(&mut self, e: &mut Expr, offset: i64) {
        let pr = e.position_range();
        let last_closing = self.last_closing;
        let (orig, orig_expr_set, end_pos): (&mut i64, bool, &mut Pos) = match e {
            Expr::VectorSelector(s) => (&mut s.original_offset, s.original_offset_expr.is_some(), &mut s.pos_range.end),
            Expr::MatrixSelector(s) => match &mut s.vector_selector {
                Expr::VectorSelector(vs) => {
                    (&mut vs.original_offset, vs.original_offset_expr.is_some(), &mut s.end_pos)
                }
                _ => {
                    self.add_parse_err(pr, "ranges only allowed for vector selectors".into());
                    return;
                }
            },
            Expr::Subquery(s) => (&mut s.original_offset, s.original_offset_expr.is_some(), &mut s.end_pos),
            _ => {
                self.add_parse_err(
                    pr,
                    "offset modifier must be preceded by an instant vector selector or range vector selector or a subquery"
                        .into(),
                );
                return;
            }
        };
        // It is already ensured by parseDuration func that there never will be a zero offset modifier.
        let dup = *orig != 0 || orig_expr_set;
        if !dup {
            *orig = offset;
        }
        *end_pos = last_closing;
        if dup {
            self.add_parse_err(pr, "offset may not be set multiple times".into());
        }
    }

    /// Go `addOffsetExpr`.
    fn add_offset_expr(&mut self, e: &mut Expr, de: Box<DurationExpr>) {
        let pr = e.position_range();
        let last_closing = self.last_closing;
        let (orig, orig_expr, end_pos): (i64, &mut Option<Box<DurationExpr>>, &mut Pos) = match e {
            Expr::VectorSelector(s) => (s.original_offset, &mut s.original_offset_expr, &mut s.pos_range.end),
            Expr::MatrixSelector(s) => match &mut s.vector_selector {
                Expr::VectorSelector(vs) => (vs.original_offset, &mut vs.original_offset_expr, &mut s.end_pos),
                _ => {
                    self.add_parse_err(pr, "ranges only allowed for vector selectors".into());
                    return;
                }
            },
            Expr::Subquery(s) => (s.original_offset, &mut s.original_offset_expr, &mut s.end_pos),
            _ => {
                self.add_parse_err(
                    pr,
                    "offset modifier must be preceded by an instant vector selector or range vector selector or a subquery"
                        .into(),
                );
                return;
            }
        };
        let dup = orig != 0 || orig_expr.is_some();
        if !dup {
            *orig_expr = Some(de);
        }
        *end_pos = last_closing;
        if dup {
            self.add_parse_err(pr, "offset may not be set multiple times".into());
        }
    }

    /// Go `setAnchored` and `setSmoothed`.
    fn set_anchored_or_smoothed(&mut self, e: &mut Expr, anchored: bool) {
        let pr = e.position_range();
        let name = if anchored { "anchored" } else { "smoothed" };
        let vs = match e {
            Expr::VectorSelector(s) => s,
            Expr::MatrixSelector(s) => match &mut s.vector_selector {
                Expr::VectorSelector(vs) => vs,
                _ => return,
            },
            Expr::Subquery(_) => {
                self.add_parse_err(pr, format!("{} modifier is not supported for subqueries", name));
                return;
            }
            _ => {
                self.add_parse_err(pr, format!("{} modifier not implemented", name));
                return;
            }
        };
        let conflict = if anchored {
            vs.anchored = true;
            vs.smoothed
        } else {
            vs.smoothed = true;
            vs.anchored
        };
        if conflict {
            self.add_parse_err(pr, "anchored and smoothed modifiers cannot be used together".into());
        }
    }

    fn set_anchored(&mut self, e: &mut Expr) {
        self.set_anchored_or_smoothed(e, true);
    }

    fn set_smoothed(&mut self, e: &mut Expr) {
        self.set_anchored_or_smoothed(e, false);
    }

    /// Go `getAtModifierVars`: the timestamp, preprocessor and end position fields of the node, or None after
    /// recording an error.
    fn at_modifier_vars<'e>(
        &mut self,
        e: &'e mut Expr,
    ) -> Option<(&'e mut Option<i64>, &'e mut ItemType, &'e mut Pos)> {
        let pr = e.position_range();
        let vars = match e {
            Expr::VectorSelector(s) => Some((&mut s.timestamp, &mut s.start_or_end, &mut s.pos_range.end)),
            Expr::MatrixSelector(s) => match &mut s.vector_selector {
                Expr::VectorSelector(vs) => Some((&mut vs.timestamp, &mut vs.start_or_end, &mut s.end_pos)),
                _ => {
                    self.add_parse_err(pr, "ranges only allowed for vector selectors".into());
                    None
                }
            },
            Expr::Subquery(s) => Some((&mut s.timestamp, &mut s.start_or_end, &mut s.end_pos)),
            _ => {
                self.add_parse_err(
                    pr,
                    "@ modifier must be preceded by an instant vector selector or range vector selector or a subquery"
                        .into(),
                );
                None
            }
        };
        let (ts, preproc, end) = vars?;
        if ts.is_some() || *preproc == START || *preproc == END {
            self.add_parse_err(pr, "@ <timestamp> may not be set multiple times".into());
            return None;
        }
        Some((ts, preproc, end))
    }

    /// Go `setTimestamp`.
    fn set_timestamp(&mut self, e: &mut Expr, ts: f64) {
        if ts.is_infinite() || ts.is_nan() || ts >= i64::MAX as f64 || ts <= i64::MIN as f64 {
            let pr = e.position_range();
            self.add_parse_err(
                pr,
                format!("timestamp out of bounds for @ modifier: {}", model::histogram::gofmt::f6(ts)),
            );
        }
        let last_closing = self.last_closing;
        if let Some((tsp, _, end)) = self.at_modifier_vars(e) {
            *tsp = Some(go_f64_to_i64((ts * 1000.0).round()));
            *end = last_closing;
        }
    }

    /// Go `setAtModifierPreprocessor`.
    fn set_at_modifier_preprocessor(&mut self, e: &mut Expr, op: &Item<'_>) {
        let last_closing = self.last_closing;
        if let Some((_, preproc, end)) = self.at_modifier_vars(e) {
            *preproc = op.typ;
            *end = last_closing;
        }
    }

    /// Go `applyUnaryOpToDurationExpr`.
    fn apply_unary_op_to_duration_expr(&mut self, op: &Item<'_>, e: Expr, wrapped: bool) -> Expr {
        let e = if wrapped {
            let pr = e.position_range();
            wrap_paren_duration_expr(e, pr.start, pr.end)
        } else {
            e
        };
        match e {
            Expr::Duration(de) => {
                if op.typ == SUB {
                    return Expr::Duration(Box::new(DurationExpr {
                        op: SUB,
                        rhs: Some(Expr::Duration(de)),
                        start_pos: op.pos,
                        ..Default::default()
                    }));
                }
                Expr::Duration(de)
            }
            Expr::NumberLiteral(mut nl) => {
                if op.typ == SUB {
                    nl.val *= -1.0;
                }
                if duration_literal_out_of_range(nl.val) {
                    self.add_parse_err(op.position_range(), "duration out of range".into());
                    return zero_literal();
                }
                nl.pos_range.start = op.pos;
                Expr::NumberLiteral(nl)
            }
            _ => {
                self.add_parse_err(op.position_range(), "expected number literal or duration expression".into());
                zero_literal()
            }
        }
    }
}

/// Go `wrapParenDurationExpr`: marks a duration expression as parenthesised so that `String()` round-trips. A
/// number literal is wrapped in a unary-plus duration expression.
fn wrap_paren_duration_expr(e: Expr, start: Pos, end: Pos) -> Expr {
    match e {
        Expr::Duration(mut de) => {
            de.wrapped = true;
            Expr::Duration(de)
        }
        e => Expr::Duration(Box::new(DurationExpr {
            op: ADD,
            rhs: Some(e),
            wrapped: true,
            start_pos: start,
            end_pos: end,
            lhs: None,
        })),
    }
}

/// Splits a duration node into the literal duration (nanoseconds) and the duration expression.
fn split_duration(e: Expr) -> (i64, Option<Box<DurationExpr>>) {
    match e {
        Expr::NumberLiteral(nl) => (seconds_to_duration(nl.val), None),
        Expr::Duration(de) => (0, Some(de)),
        _ => (0, None),
    }
}

/// Go `%v` of a histogram description value.
fn desc_val_str(v: &DescVal) -> String {
    match v {
        DescVal::Int(i) => i.to_string(),
        DescVal::Float(f) => model::histogram::gofmt::g(*f),
        DescVal::Floats(f) => model::histogram::gofmt::g_slice(f),
        DescVal::Hint(t) => format!("{{{} 0 }}", t.0),
    }
}

#[cfg(test)]
mod tests {
    use super::super::generated::RULES;

    /// The rule numbers that `reduce` handles, with the rule text it expects. A grammar change upstream moves
    /// the numbers; this test then names the rules to check.
    #[test]
    fn rule_numbers_match_grammar() {
        let expect: &[(usize, &str)] = &[
            (1, "start: START_METRIC metric"),
            (3, "start: START_EXPRESSION EOF"),
            (4, "start: START_EXPRESSION expr"),
            (5, "start: START_METRIC_SELECTOR vector_selector"),
            (7, "start: error"),
            (23, "aggregate_expr: aggregate_op aggregate_modifier function_call_body"),
            (24, "aggregate_expr: aggregate_op function_call_body aggregate_modifier"),
            (25, "aggregate_expr: aggregate_op function_call_body"),
            (26, "aggregate_expr: aggregate_op error"),
            (27, "aggregate_modifier: BY grouping_labels"),
            (28, "aggregate_modifier: WITHOUT grouping_labels"),
            (29, "binary_expr: expr ADD bin_modifier expr"),
            (46, "binary_expr: expr SUB bin_modifier expr"),
            (48, "bool_modifier:"),
            (49, "bool_modifier: BOOL"),
            (50, "on_or_ignoring: bool_modifier IGNORING grouping_labels"),
            (51, "on_or_ignoring: bool_modifier ON grouping_labels"),
            (54, "group_modifiers: on_or_ignoring GROUP_LEFT maybe_grouping_labels"),
            (55, "group_modifiers: on_or_ignoring GROUP_RIGHT maybe_grouping_labels"),
            (57, "fill_modifiers: group_modifiers FILL fill_value"),
            (58, "fill_modifiers: group_modifiers FILL_LEFT fill_value"),
            (59, "fill_modifiers: group_modifiers FILL_RIGHT fill_value"),
            (60, "fill_modifiers: group_modifiers FILL_LEFT fill_value FILL_RIGHT fill_value"),
            (61, "fill_modifiers: group_modifiers FILL_RIGHT fill_value FILL_LEFT fill_value"),
            (62, "grouping_labels: LEFT_PAREN grouping_label_list RIGHT_PAREN"),
            (63, "grouping_labels: LEFT_PAREN grouping_label_list COMMA RIGHT_PAREN"),
            (64, "grouping_labels: LEFT_PAREN RIGHT_PAREN"),
            (65, "grouping_labels: error"),
            (66, "grouping_label_list: grouping_label_list COMMA grouping_label"),
            (67, "grouping_label_list: grouping_label"),
            (68, "grouping_label_list: grouping_label_list error"),
            (69, "grouping_label: maybe_label"),
            (70, "grouping_label: STRING"),
            (71, "grouping_label: error"),
            (72, "fill_value: LEFT_PAREN number_duration_literal RIGHT_PAREN"),
            (73, "fill_value: LEFT_PAREN unary_op number_duration_literal RIGHT_PAREN"),
            (74, "function_call: IDENTIFIER function_call_body"),
            (78, "function_call: max_of_min_of function_call_body"),
            (79, "function_call_body: LEFT_PAREN function_call_args RIGHT_PAREN"),
            (80, "function_call_body: LEFT_PAREN RIGHT_PAREN"),
            (81, "function_call_args: function_call_args COMMA expr"),
            (82, "function_call_args: expr"),
            (83, "function_call_args: function_call_args COMMA"),
            (84, "paren_expr: LEFT_PAREN expr RIGHT_PAREN"),
            (85, "positive_duration_expr: duration_expr"),
            (86, "offset_expr: expr OFFSET offset_duration_expr"),
            (87, "offset_expr: expr OFFSET error"),
            (88, "anchored_expr: expr ANCHORED"),
            (89, "smoothed_expr: expr SMOOTHED"),
            (90, "step_invariant_expr: expr AT signed_or_unsigned_number"),
            (91, "step_invariant_expr: expr AT at_modifier_preprocessors LEFT_PAREN RIGHT_PAREN"),
            (92, "step_invariant_expr: expr AT error"),
            (95, "matrix_selector: expr LEFT_BRACKET positive_duration_expr RIGHT_BRACKET"),
            (96, "subquery_expr: expr LEFT_BRACKET positive_duration_expr COLON positive_duration_expr RIGHT_BRACKET"),
            (97, "subquery_expr: expr LEFT_BRACKET positive_duration_expr COLON RIGHT_BRACKET"),
            (98, "subquery_expr: expr LEFT_BRACKET positive_duration_expr COLON positive_duration_expr error"),
            (99, "subquery_expr: expr LEFT_BRACKET positive_duration_expr COLON error"),
            (100, "subquery_expr: expr LEFT_BRACKET positive_duration_expr error"),
            (101, "subquery_expr: expr LEFT_BRACKET error"),
            (102, "unary_expr: unary_op expr"),
            (103, "vector_selector: metric_identifier label_matchers"),
            (104, "vector_selector: metric_identifier"),
            (105, "vector_selector: label_matchers"),
            (106, "label_matchers: LEFT_BRACE label_match_list RIGHT_BRACE"),
            (107, "label_matchers: LEFT_BRACE label_match_list COMMA RIGHT_BRACE"),
            (108, "label_matchers: LEFT_BRACE RIGHT_BRACE"),
            (109, "label_match_list: label_match_list COMMA label_matcher"),
            (110, "label_match_list: label_matcher"),
            (111, "label_match_list: label_match_list error"),
            (112, "label_matcher: IDENTIFIER match_op STRING"),
            (113, "label_matcher: string_identifier match_op STRING"),
            (114, "label_matcher: string_identifier"),
            (115, "label_matcher: string_identifier match_op error"),
            (116, "label_matcher: IDENTIFIER match_op error"),
            (117, "label_matcher: IDENTIFIER error"),
            (118, "label_matcher: error"),
            (119, "metric: metric_identifier label_set"),
            (154, "label_set: LEFT_BRACE label_set_list RIGHT_BRACE"),
            (155, "label_set: LEFT_BRACE label_set_list COMMA RIGHT_BRACE"),
            (156, "label_set: LEFT_BRACE RIGHT_BRACE"),
            (157, "label_set:"),
            (158, "label_set_list: label_set_list COMMA label_set_item"),
            (159, "label_set_list: label_set_item"),
            (160, "label_set_list: label_set_list error"),
            (161, "label_set_item: IDENTIFIER EQL STRING"),
            (162, "label_set_item: string_identifier EQL STRING"),
            (163, "label_set_item: string_identifier"),
            (164, "label_set_item: IDENTIFIER EQL error"),
            (165, "label_set_item: string_identifier EQL error"),
            (166, "label_set_item: IDENTIFIER error"),
            (167, "label_set_item: error"),
            (168, "series_description: metric series_values"),
            (169, "series_values:"),
            (170, "series_values: series_values SPACE series_item"),
            (171, "series_values: series_values SPACE"),
            (172, "series_values: error"),
            (173, "series_item: BLANK"),
            (174, "series_item: BLANK TIMES uint"),
            (175, "series_item: series_value"),
            (176, "series_item: series_value TIMES uint"),
            (177, "series_item: series_value signed_number TIMES uint"),
            (178, "series_item: histogram_series_value"),
            (179, "series_item: histogram_series_value TIMES uint"),
            (180, "series_item: histogram_series_value ADD histogram_series_value TIMES uint"),
            (181, "series_item: histogram_series_value SUB histogram_series_value TIMES uint"),
            (182, "series_value: IDENTIFIER"),
            (185, "histogram_series_value: OPEN_HIST histogram_desc_map SPACE CLOSE_HIST"),
            (186, "histogram_series_value: OPEN_HIST histogram_desc_map CLOSE_HIST"),
            (187, "histogram_series_value: OPEN_HIST SPACE CLOSE_HIST"),
            (188, "histogram_series_value: OPEN_HIST CLOSE_HIST"),
            (189, "histogram_desc_map: histogram_desc_map SPACE histogram_desc_item"),
            (191, "histogram_desc_map: histogram_desc_map error"),
            (192, "histogram_desc_item: SCHEMA_DESC COLON int"),
            (193, "histogram_desc_item: SUM_DESC COLON signed_or_unsigned_number"),
            (194, "histogram_desc_item: COUNT_DESC COLON signed_or_unsigned_number"),
            (195, "histogram_desc_item: ZERO_BUCKET_DESC COLON signed_or_unsigned_number"),
            (196, "histogram_desc_item: ZERO_BUCKET_WIDTH_DESC COLON number"),
            (197, "histogram_desc_item: CUSTOM_VALUES_DESC COLON bucket_set"),
            (198, "histogram_desc_item: BUCKETS_DESC COLON bucket_set"),
            (199, "histogram_desc_item: OFFSET_DESC COLON int"),
            (200, "histogram_desc_item: NEGATIVE_BUCKETS_DESC COLON bucket_set"),
            (201, "histogram_desc_item: NEGATIVE_OFFSET_DESC COLON int"),
            (202, "histogram_desc_item: COUNTER_RESET_HINT_DESC COLON counter_reset_hint"),
            (203, "bucket_set: LEFT_BRACKET bucket_set_list SPACE RIGHT_BRACKET"),
            (204, "bucket_set: LEFT_BRACKET bucket_set_list RIGHT_BRACKET"),
            (205, "bucket_set_list: bucket_set_list SPACE signed_or_unsigned_number"),
            (206, "bucket_set_list: signed_or_unsigned_number"),
            (270, "number_duration_literal: NUMBER"),
            (271, "number_duration_literal: DURATION"),
            (272, "number: NUMBER"),
            (273, "number: DURATION"),
            (274, "signed_number: ADD number"),
            (275, "signed_number: SUB number"),
            (278, "uint: NUMBER"),
            (279, "int: SUB uint"),
            (280, "int: uint"),
            (281, "string_literal: STRING"),
            (282, "string_identifier: STRING"),
            (283, "maybe_grouping_labels:"),
            (285, "offset_duration_expr: number_duration_literal"),
            (286, "offset_duration_expr: unary_op number_duration_literal"),
            (287, "offset_duration_expr: STEP LEFT_PAREN RIGHT_PAREN"),
            (288, "offset_duration_expr: RANGE LEFT_PAREN RIGHT_PAREN"),
            (289, "offset_duration_expr: unary_op STEP LEFT_PAREN RIGHT_PAREN"),
            (290, "offset_duration_expr: unary_op RANGE LEFT_PAREN RIGHT_PAREN"),
            (291, "offset_duration_expr: max_of_min_of LEFT_PAREN duration_expr COMMA duration_expr RIGHT_PAREN"),
            (
                292,
                "offset_duration_expr: unary_op max_of_min_of LEFT_PAREN duration_expr COMMA duration_expr RIGHT_PAREN",
            ),
            (293, "offset_duration_expr: unary_op LEFT_PAREN duration_expr RIGHT_PAREN"),
            (297, "duration_expr: number_duration_literal"),
            (298, "duration_expr: unary_op duration_expr"),
            (299, "duration_expr: duration_expr ADD duration_expr"),
            (302, "duration_expr: duration_expr DIV duration_expr"),
            (303, "duration_expr: duration_expr MOD duration_expr"),
            (304, "duration_expr: duration_expr POW duration_expr"),
            (305, "duration_expr: STEP LEFT_PAREN RIGHT_PAREN"),
            (306, "duration_expr: RANGE LEFT_PAREN RIGHT_PAREN"),
            (307, "duration_expr: max_of_min_of LEFT_PAREN duration_expr COMMA duration_expr RIGHT_PAREN"),
            (309, "paren_duration_expr: LEFT_PAREN duration_expr RIGHT_PAREN"),
        ];
        assert_eq!(RULES.len(), 310);
        for (n, text) in expect {
            assert_eq!(RULES[*n], *text, "rule {}", n);
        }
    }
}
