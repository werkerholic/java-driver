/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * A customizable component to format instances of {@link Statement}.
 * <p/>
 * Instances of {@link StatementFormatter} can be obtained
 * through the {@link #builder()} method.
 * <p/>
 * Instances of this class are thread-safe.
 */
public final class StatementFormatter {

    public static final StatementFormatter DEFAULT_INSTANCE = StatementFormatter.builder().build();

    /**
     * Creates a new {@link StatementFormatter.Builder} instance.
     *
     * @return the new StatementFormatter builder.
     */
    public static StatementFormatter.Builder builder() {
        return new StatementFormatter.Builder();
    }

    /**
     * The desired statement format verbosity.
     * <p/>
     * This should be used as a guideline as to how much information
     * about the statement should be extracted and formatted.
     */
    public enum StatementFormatVerbosity {

        // the enum order matters

        /**
         * Formatters should only print a basic information in summarized form.
         */
        ABRIDGED,

        /**
         * Formatters should print basic information in summarized form,
         * and the statement's query string, if available.
         * <p/>
         * For batch statements, this verbosity level should
         * allow formatters to print information about the batch's
         * inner statements.
         */
        NORMAL,

        /**
         * Formatters should print full information, including
         * the statement's query string, if available,
         * and the statement's bound values, if available.
         */
        EXTENDED

    }

    /**
     * A statement printer is responsible for printing a specific type of {@link Statement statement},
     * with a given {@link StatementFormatVerbosity verbosity level},
     * and using a given {@link StatementWriter statement writer}.
     *
     * @param <S> The type of statement that this printer handles
     */
    public interface StatementPrinter<S extends Statement> {

        /**
         * The concrete {@link Statement} subclass that this printer handles.
         * <p/>
         * In case of subtype polymorphism, if this printer
         * handles more than one concrete subclass,
         * the most specific common ancestor should be returned here.
         *
         * @return The concrete {@link Statement} subclass that this printer handles.
         */
        Class<S> getSupportedStatementClass();

        /**
         * Prints the given {@link Statement statement},
         * using the given {@link StatementWriter statement writer} and
         * the given {@link StatementFormatVerbosity verbosity level}.
         *
         * @param statement the statement to print
         * @param out       the writer to use
         * @param verbosity the verbosity to use
         */
        void print(S statement, StatementWriter out, StatementFormatVerbosity verbosity);

    }

    /**
     * Thrown when a {@link StatementFormatter} encounters an error
     * while formatting a {@link Statement}.
     */
    public static class StatementFormatException extends RuntimeException {

        private final Statement statement;
        private final StatementFormatVerbosity verbosity;
        private final ProtocolVersion protocolVersion;
        private final CodecRegistry codecRegistry;

        public StatementFormatException(Statement statement, StatementFormatVerbosity verbosity, ProtocolVersion protocolVersion, CodecRegistry codecRegistry, Throwable t) {
            super(t);
            this.statement = statement;
            this.verbosity = verbosity;
            this.protocolVersion = protocolVersion;
            this.codecRegistry = codecRegistry;
        }

        /**
         * @return The statement that failed to format.
         */
        public Statement getStatement() {
            return statement;
        }

        /**
         * @return The requested verbosity.
         */
        public StatementFormatVerbosity getVerbosity() {
            return verbosity;
        }

        /**
         * @return The protocol version in use.
         */
        public ProtocolVersion getProtocolVersion() {
            return protocolVersion;
        }

        /**
         * @return The codec registry in use.
         */
        public CodecRegistry getCodecRegistry() {
            return codecRegistry;
        }
    }

    /**
     * A set of user-defined symbols that {@link StatementPrinter printers}
     * are required to use when formatting statements.
     * <p/>
     * This class is NOT thread-safe.
     */
    public static final class StatementFormatterSymbols {

        private String summaryStart = " [";
        private String summaryEnd = "]";
        private String boundValuesCount = "%s bound values";
        private String statementsCount = "%s inner statements";
        private String consistency = "CL=%s";
        private String serialConsistency = "SCL=%s";
        private String defaultTimestamp = "defaultTimestamp=%s";
        private String readTimeoutMillis = "readTimeoutMillis=%s";
        private String idempotent = "idempotent=%s";
        private String queryStringStart = ": ";
        private String queryStringEnd = " ";
        private String boundValuesStart = "{ ";
        private String boundValuesEnd = " }";
        private String outgoingPayloadStart = "< ";
        private String outgoingPayloadEnd = " >";
        private String truncatedOutput = "...";
        private String nullValue = "<NULL>";
        private String unsetValue = "<UNSET>";
        private String listElementSeparator = ", ";
        private String nameValueSeparator = " : ";

        /**
         * Sets the template to use when printing a statement's
         * summary start.
         *
         * @param summaryStart the template to use.
         * @return this
         */
        public StatementFormatterSymbols setSummaryStart(String summaryStart) {
            this.summaryStart = summaryStart;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * summary end.
         *
         * @param summaryEnd the template to use.
         * @return this
         */
        public StatementFormatterSymbols setSummaryEnd(String summaryEnd) {
            this.summaryEnd = summaryEnd;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * bound values count.
         * <p/>
         * The template should be a {@link String#format(String, Object...) String.format template} accepting one parameter.
         *
         * @param boundValuesCount the template to use.
         * @return this
         */
        public StatementFormatterSymbols setBoundValuesCount(String boundValuesCount) {
            this.boundValuesCount = boundValuesCount;
            return this;
        }

        /**
         * Sets the template to use when printing a batch's
         * inner statements count.
         * <p/>
         * The template should be a {@link String#format(String, Object...) String.format template} accepting one parameter.
         *
         * @param statementsCount the template to use.
         * @return this
         */
        public StatementFormatterSymbols setStatementsCount(String statementsCount) {
            this.statementsCount = statementsCount;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * consistency level.
         * <p/>
         * The template should be a {@link String#format(String, Object...) String.format template} accepting one parameter.
         *
         * @param consistency the template to use.
         * @return this
         */
        public StatementFormatterSymbols setConsistency(String consistency) {
            this.consistency = consistency;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * serial consistency level.
         * <p/>
         * The template should be a {@link String#format(String, Object...) String.format template} accepting one parameter.
         *
         * @param serialConsistency the template to use.
         * @return this
         */
        public StatementFormatterSymbols setSerialConsistency(String serialConsistency) {
            this.serialConsistency = serialConsistency;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * default timestamp.
         * <p/>
         * The template should be a {@link String#format(String, Object...) String.format template} accepting one parameter.
         *
         * @param defaultTimestamp the template to use.
         * @return this
         */
        public StatementFormatterSymbols setDefaultTimestamp(String defaultTimestamp) {
            this.defaultTimestamp = defaultTimestamp;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * read timeout value.
         * <p/>
         * The template should be a {@link String#format(String, Object...) String.format template} accepting one parameter.
         *
         * @param readTimeoutMillis the template to use.
         * @return this
         */
        public StatementFormatterSymbols setReadTimeoutMillis(String readTimeoutMillis) {
            this.readTimeoutMillis = readTimeoutMillis;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * idempotence flag.
         * <p/>
         * The template should be a {@link String#format(String, Object...) String.format template} accepting one parameter.
         *
         * @param idempotent the template to use.
         * @return this
         */
        public StatementFormatterSymbols setIdempotent(String idempotent) {
            this.idempotent = idempotent;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * query string start.
         *
         * @param queryStringStart the template to use.
         * @return this
         */
        public StatementFormatterSymbols setQueryStringStart(String queryStringStart) {
            this.queryStringStart = queryStringStart;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * query string end.
         *
         * @param queryStringEnd the template to use.
         * @return this
         */
        public StatementFormatterSymbols setQueryStringEnd(String queryStringEnd) {
            this.queryStringEnd = queryStringEnd;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * bound values start.
         *
         * @param boundValuesStart the template to use.
         * @return this
         */
        public StatementFormatterSymbols setBoundValuesStart(String boundValuesStart) {
            this.boundValuesStart = boundValuesStart;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * bound values end.
         *
         * @param boundValuesEnd the template to use.
         * @return this
         */
        public StatementFormatterSymbols setBoundValuesEnd(String boundValuesEnd) {
            this.boundValuesEnd = boundValuesEnd;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * outgoing payload start.
         *
         * @param outgoingPayloadStart the template to use.
         * @return this
         */
        public StatementFormatterSymbols setOutgoingPayloadStart(String outgoingPayloadStart) {
            this.outgoingPayloadStart = outgoingPayloadStart;
            return this;
        }

        /**
         * Sets the template to use when printing a statement's
         * outgoing payload end.
         *
         * @param outgoingPayloadEnd the template to use.
         * @return this
         */
        public StatementFormatterSymbols setOutgoingPayloadEnd(String outgoingPayloadEnd) {
            this.outgoingPayloadEnd = outgoingPayloadEnd;
            return this;
        }

        /**
         * Sets the template to use when the output is truncated
         * because it exceeds some imposed length limit.
         *
         * @param truncatedOutput the template to use.
         * @return this
         */
        public StatementFormatterSymbols setTruncatedOutput(String truncatedOutput) {
            this.truncatedOutput = truncatedOutput;
            return this;
        }

        /**
         * Sets the template to use when printing a null value.
         *
         * @param nullValue the template to use.
         * @return this
         */
        public StatementFormatterSymbols setNullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        /**
         * Sets the template to use when printing an unset (bound) value.
         *
         * @param unsetValue the template to use.
         * @return this
         */
        public StatementFormatterSymbols setUnsetValue(String unsetValue) {
            this.unsetValue = unsetValue;
            return this;
        }

        /**
         * Sets the list element separator.
         *
         * @param listElementSeparator the list element separator to use.
         * @return this
         */
        public StatementFormatterSymbols setListElementSeparator(String listElementSeparator) {
            this.listElementSeparator = listElementSeparator;
            return this;
        }

        /**
         * Sets the name-value separator.
         *
         * @param nameValueSeparator the name-value separator to use.
         * @return this
         */
        public StatementFormatterSymbols setNameValueSeparator(String nameValueSeparator) {
            this.nameValueSeparator = nameValueSeparator;
            return this;
        }
    }

    /**
     * A set of user-defined limitation rules that {@link StatementPrinter printers}
     * should strive to comply with when formatting statements.
     * <p/>
     * Limits defined in this class should be considered on a per-statement basis;
     * i.e. if the maximum query string length is 100 and the statement to format
     * is a {@link BatchStatement} with 5 inner statements, each inner statement
     * should be allowed to print a maximum of 100 characters of its query string.
     * <p/>
     * This class is NOT thread-safe.
     */
    public static final class StatementFormatterLimits {

        /**
         * A special value that conveys the notion of "unlimited".
         * All fields in this class accept this value.
         */
        public static final int UNLIMITED = -1;

        public static final int DEFAULT_MAX_QUERY_STRING_LENGTH = 500;
        public static final int DEFAULT_MAX_BOUND_VALUE_LENGTH = 50;
        public static final int DEFAULT_MAX_BOUND_VALUES = 10;
        public static final int DEFAULT_MAX_INNER_STATEMENTS = 5;
        public static final int DEFAULT_MAX_OUTGOING_PAYLOAD_ENTRIES = 10;
        public static final int DEFAULT_MAX_OUTGOING_PAYLOAD_VALUE_LENGTH = 50;

        private int maxQueryStringLength = DEFAULT_MAX_QUERY_STRING_LENGTH;
        private int maxBoundValueLength = DEFAULT_MAX_BOUND_VALUE_LENGTH;
        private int maxBoundValues = DEFAULT_MAX_BOUND_VALUES;
        private int maxInnerStatements = DEFAULT_MAX_INNER_STATEMENTS;
        private int maxOutgoingPayloadEntries = DEFAULT_MAX_OUTGOING_PAYLOAD_ENTRIES;
        private int maxOutgoingPayloadValueLength = DEFAULT_MAX_OUTGOING_PAYLOAD_VALUE_LENGTH;

        /**
         * Sets the maximum length allowed for query strings.
         * The default is {@value DEFAULT_MAX_QUERY_STRING_LENGTH}.
         * <p/>
         * If the query string length exceeds this threshold,
         * printers should append the
         * {@link StatementFormatterSymbols#setTruncatedOutput(String) truncatedOutput} symbol.
         *
         * @param maxQueryStringLength the maximum length allowed for query strings.
         * @throws IllegalArgumentException if the value is not > 0, or {@value UNLIMITED} (unlimited).
         */
        public StatementFormatterLimits setMaxQueryStringLength(int maxQueryStringLength) {
            if (maxQueryStringLength <= 0 && maxQueryStringLength != UNLIMITED)
                throw new IllegalArgumentException("Invalid maxQueryStringLength, should be > 0 or -1 (unlimited), got " + maxQueryStringLength);
            this.maxQueryStringLength = maxQueryStringLength;
            return this;
        }

        /**
         * Sets the maximum length, in numbers of printed characters,
         * allowed for a single bound value.
         * The default is {@value DEFAULT_MAX_BOUND_VALUE_LENGTH}.
         * <p/>
         * If the bound value length exceeds this threshold,
         * printers should append the
         * {@link StatementFormatterSymbols#setTruncatedOutput(String) truncatedOutput} symbol.
         *
         * @param maxBoundValueLength the maximum length, in numbers of printed characters,
         * allowed for a single bound value.
         * @throws IllegalArgumentException if the value is not > 0, or {@value UNLIMITED} (unlimited).
         */
        public StatementFormatterLimits setMaxBoundValueLength(int maxBoundValueLength) {
            if (maxBoundValueLength <= 0 && maxBoundValueLength != UNLIMITED)
                throw new IllegalArgumentException("Invalid maxBoundValueLength, should be > 0 or -1 (unlimited), got " + maxBoundValueLength);
            this.maxBoundValueLength = maxBoundValueLength;
            return this;
        }

        /**
         * Sets the maximum number of printed bound values.
         * The default is {@value DEFAULT_MAX_BOUND_VALUES}.
         * <p/>
         * If the number of bound values exceeds this threshold,
         * printers should append the
         * {@link StatementFormatterSymbols#setTruncatedOutput(String) truncatedOutput} symbol.
         *
         * @param maxBoundValues the maximum number of printed bound values.
         * @throws IllegalArgumentException if the value is not > 0, or {@value UNLIMITED} (unlimited).
         */
        public StatementFormatterLimits setMaxBoundValues(int maxBoundValues) {
            if (maxBoundValues <= 0 && maxBoundValues != UNLIMITED)
                throw new IllegalArgumentException("Invalid maxBoundValues, should be > 0 or -1 (unlimited), got " + maxBoundValues);
            this.maxBoundValues = maxBoundValues;
            return this;
        }

        /**
         * Sets the maximum number of printed inner statements
         * of a {@link BatchStatement}.
         * The default is {@value DEFAULT_MAX_INNER_STATEMENTS}.
         * Setting this value to zero should disable the printing
         * of inner statements.
         * <p/>
         * If the number of inner statements exceeds this threshold,
         * printers should append the
         * {@link StatementFormatterSymbols#setTruncatedOutput(String) truncatedOutput} symbol.
         * <p/>
         * If the statement to format is not a batch statement,
         * then this setting should be ignored.
         *
         * @param maxInnerStatements the maximum number of printed inner statements
         * of a {@link BatchStatement}.
         * @throws IllegalArgumentException if the value is not >= 0, or {@value UNLIMITED} (unlimited).
         */
        public StatementFormatterLimits setMaxInnerStatements(int maxInnerStatements) {
            if (maxInnerStatements < 0 && maxInnerStatements != UNLIMITED)
                throw new IllegalArgumentException("Invalid maxInnerStatements, should be >= 0 or -1 (unlimited), got " + maxInnerStatements);
            this.maxInnerStatements = maxInnerStatements;
            return this;
        }

        /**
         * Sets the maximum number of printed outgoing payload entries.
         * The default is {@value DEFAULT_MAX_OUTGOING_PAYLOAD_ENTRIES}.
         * <p/>
         * If the number of entries exceeds this threshold,
         * printers should append the
         * {@link StatementFormatterSymbols#setTruncatedOutput(String) truncatedOutput} symbol.
         *
         * @param maxOutgoingPayloadEntries the maximum number of printed outgoing payload entries.
         * @throws IllegalArgumentException if the value is not > 0, or {@value UNLIMITED} (unlimited).
         */
        public StatementFormatterLimits setMaxOutgoingPayloadEntries(int maxOutgoingPayloadEntries) {
            if (maxOutgoingPayloadEntries <= 0 && maxOutgoingPayloadEntries != UNLIMITED)
                throw new IllegalArgumentException("Invalid maxOutgoingPayloadEntries, should be > 0 or -1 (unlimited), got " + maxOutgoingPayloadEntries);
            this.maxOutgoingPayloadEntries = maxOutgoingPayloadEntries;
            return this;
        }

        /**
         * Sets the maximum length, in bytes, allowed for a single outgoing payload value.
         * The default is {@value DEFAULT_MAX_OUTGOING_PAYLOAD_VALUE_LENGTH}.
         * <p/>
         * If the payload value length in bytes exceeds this threshold,
         * printers should append the
         * {@link StatementFormatterSymbols#setTruncatedOutput(String) truncatedOutput} symbol.
         *
         * @param maxOutgoingPayloadValueLength the maximum length, in bytes, allowed for a single outgoing payload value.
         * @throws IllegalArgumentException if the value is not > 0, or {@value UNLIMITED} (unlimited).
         */
        public StatementFormatterLimits setMaxOutgoingPayloadValueLength(int maxOutgoingPayloadValueLength) {
            if (maxOutgoingPayloadValueLength <= 0 && maxOutgoingPayloadValueLength != UNLIMITED)
                throw new IllegalArgumentException("Invalid maxOutgoingPayloadValueLength, should be > 0 or -1 (unlimited), got " + maxOutgoingPayloadValueLength);
            this.maxOutgoingPayloadValueLength = maxOutgoingPayloadValueLength;
            return this;
        }
    }

    /**
     * A registry for {@link StatementPrinter statement printers}.
     * <p/>
     * This class is thread-safe.
     */
    public static final class StatementPrinterRegistry {

        private final LoadingCache<Class<? extends Statement>, StatementPrinter> printers;

        private StatementPrinterRegistry() {
            printers = CacheBuilder.newBuilder().build(new CacheLoader<Class<? extends Statement>, StatementPrinter>() {
                @SuppressWarnings({"raw", "unchecked"})
                @Override
                public StatementPrinter load(Class key) throws Exception {
                    StatementPrinter printer = null;
                    while (printer == null) {
                        key = key.getSuperclass();
                        printer = printers.get(key);
                    }
                    return printer;
                }
            });

        }

        /**
         * Attempts to locate the best {@link StatementPrinter printer} for the given
         * statement.
         *
         * @param statement The statement find a printer for.
         * @return The best {@link StatementPrinter printer} for the given
         * statement. Cannot be {@code null}.
         */
        @SuppressWarnings("unchecked")
        public <S extends Statement> StatementPrinter<? super S> findPrinter(S statement) {
            try {
                return printers.get(statement.getClass());
            } catch (ExecutionException e) {
                // will never happen as long as a default statement printer is registered
                throw Throwables.propagate(e);
            }
        }

        private <S extends Statement> void register(StatementPrinter<S> printer) {
            printers.put(printer.getSupportedStatementClass(), printer);
        }

    }

    /**
     * This class exposes utility methods to help
     * {@link StatementPrinter statement printers} in formatting
     * a statement.
     * <p/>
     * Direct access to the underlying {@link StringBuilder buffer}
     * is also possible.
     * <p/>
     * Instances of this class are designed to format one single statement;
     * they keep internal counters such as the current number of printed bound values,
     * and for this reason, they should not be reused to format more than one statement.
     * When formatting more than one statement (e.g. when formatting a {@link BatchStatement} and its children),
     * one should call {@link #createChildWriter()} to create child instances of the main writer
     * to format each individual statement.
     * <p/>
     * This class is NOT thread-safe.
     */
    public static final class StatementWriter {

        private static final int MAX_EXCEEDED = -2;

        private final StringBuilder buffer;
        private final StatementPrinterRegistry printerRegistry;
        private final StatementFormatterLimits limits;
        private final StatementFormatterSymbols symbols;
        private final ProtocolVersion protocolVersion;
        private final CodecRegistry codecRegistry;
        private int remainingQueryStringChars;
        private int remainingBoundValues;

        private StatementWriter(StringBuilder buffer, StatementPrinterRegistry printerRegistry,
                                StatementFormatterLimits limits, StatementFormatterSymbols symbols,
                                ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
            this.buffer = buffer;
            this.printerRegistry = printerRegistry;
            this.limits = limits;
            this.symbols = symbols;
            this.protocolVersion = protocolVersion;
            this.codecRegistry = codecRegistry;
            remainingQueryStringChars = limits.maxQueryStringLength == StatementFormatterLimits.UNLIMITED
                    ? Integer.MAX_VALUE
                    : limits.maxQueryStringLength;
            remainingBoundValues = limits.maxBoundValues == StatementFormatterLimits.UNLIMITED
                    ? Integer.MAX_VALUE
                    : limits.maxBoundValues;
        }

        /**
         * Creates and returns a child {@link StatementWriter}.
         * <p/>
         * A child writer shares the same buffer as its parent, but has its own independent state.
         * It is most useful when dealing with inner statements in batches (each inner statement should
         * use a child writer).
         *
         * @return a child {@link StatementWriter}.
         */
        public StatementWriter createChildWriter() {
            return new StatementWriter(buffer, printerRegistry, limits, symbols, protocolVersion, codecRegistry);
        }

        /**
         * @return The {@link StatementPrinterRegistry printer registry}.
         */
        public StatementPrinterRegistry getPrinterRegistry() {
            return printerRegistry;
        }

        /**
         * @return The current limits.
         */
        public StatementFormatterLimits getLimits() {
            return limits;
        }

        /**
         * @return The current symbols.
         */
        public StatementFormatterSymbols getSymbols() {
            return symbols;
        }

        /**
         * @return The protocol version in use.
         */
        public ProtocolVersion getProtocolVersion() {
            return protocolVersion;
        }

        /**
         * @return The codec registry version in use.
         */
        public CodecRegistry getCodecRegistry() {
            return codecRegistry;
        }

        /**
         * @return The underlying buffer.
         */
        public StringBuilder getBuffer() {
            return buffer;
        }

        /**
         * @return The number of remaining query string characters that can be printed
         * without exceeding the maximum allowed length.
         */
        public int getRemainingQueryStringChars() {
            return remainingQueryStringChars;
        }

        /**
         * @return The number of remaining bound values per statement that can be printed
         * without exceeding the maximum allowed number.
         */
        public int getRemainingBoundValues() {
            return remainingBoundValues;
        }

        /**
         * @return {@code true} if the maximum query string length is exceeded, {@code false} otherwise.
         */
        public boolean maxQueryStringLengthExceeded() {
            return remainingQueryStringChars == MAX_EXCEEDED;
        }

        /**
         * @return {@code true} if the maximum number of bound values per statement is exceeded, {@code false} otherwise.
         */
        public boolean maxAppendedBoundValuesExceeded() {
            return remainingBoundValues == MAX_EXCEEDED;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setSummaryStart(String) summaryStart}"
         *
         * @return this
         */
        public StatementWriter appendSummaryStart() {
            buffer.append(symbols.summaryStart);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setSummaryEnd(String) summaryEnd}"
         *
         * @return this
         */
        public StatementWriter appendSummaryEnd() {
            buffer.append(symbols.summaryEnd);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setQueryStringStart(String) queryStringStart}"
         *
         * @return this
         */
        public StatementWriter appendQueryStringStart() {
            buffer.append(symbols.queryStringStart);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setQueryStringEnd(String) queryStringEnd}"
         *
         * @return this
         */
        public StatementWriter appendQueryStringEnd() {
            buffer.append(symbols.queryStringEnd);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setBoundValuesStart(String) boundValuesStart}"
         *
         * @return this
         */
        public StatementWriter appendBoundValuesStart() {
            buffer.append(symbols.boundValuesStart);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setBoundValuesEnd(String) boundValuesEnd}"
         *
         * @return this
         */
        public StatementWriter appendBoundValuesEnd() {
            buffer.append(symbols.boundValuesEnd);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setOutgoingPayloadStart(String) outgoingPayloadStart}"
         *
         * @return this
         */
        public StatementWriter appendOutgoingPayloadStart() {
            buffer.append(symbols.outgoingPayloadStart);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setOutgoingPayloadEnd(String) outgoingPayloadEnd}"
         *
         * @return this
         */
        public StatementWriter appendOutgoingPayloadEnd() {
            buffer.append(symbols.outgoingPayloadEnd);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setTruncatedOutput(String) truncatedOutput}"
         *
         * @return this
         */
        public StatementWriter appendTruncatedOutput() {
            buffer.append(symbols.truncatedOutput);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setNullValue(String) nullValue}"
         *
         * @return this
         */
        public StatementWriter appendNullValue() {
            buffer.append(symbols.nullValue);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setUnsetValue(String) unsetValue}"
         *
         * @return this
         */
        public StatementWriter appendUnsetValue() {
            buffer.append(symbols.unsetValue);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setListElementSeparator(String) listElementSeparator}"
         *
         * @return this
         */
        public StatementWriter appendListElementSeparator() {
            buffer.append(symbols.listElementSeparator);
            return this;
        }

        /**
         * Appends the symbol "{@link StatementFormatterSymbols#setNameValueSeparator(String) nameValueSeparator}"
         *
         * @return this
         */
        public StatementWriter appendNameValueSeparator() {
            buffer.append(symbols.nameValueSeparator);
            return this;
        }

        /**
         * Appends the statement's class name and hash code, as done by {@link Object#toString()}.
         *
         * @param statement The statement to format.
         * @return this
         */
        public StatementWriter appendClassNameAndHashCode(Statement statement) {
            String fqcn = statement.getClass().getName();
            if (fqcn.startsWith("com.datastax.driver.core.querybuilder"))
                fqcn = "BuiltStatement";
            if (fqcn.startsWith("com.datastax.driver.core.schemabuilder"))
                fqcn = "SchemaStatement";
            else if (fqcn.startsWith("com.datastax.driver.core."))
                fqcn = fqcn.substring(25);
            buffer.append(fqcn);
            buffer.append('@');
            buffer.append(Integer.toHexString(statement.hashCode()));
            return this;
        }

        public StatementWriter appendBoundValuesCount(int totalBoundValuesCount) {
            buffer.append(String.format(symbols.boundValuesCount, totalBoundValuesCount));
            return this;
        }

        public StatementWriter appendInnerStatementsCount(int numberOfInnerStatements) {
            buffer.append(String.format(symbols.statementsCount, numberOfInnerStatements));
            return this;
        }

        public StatementWriter appendConsistency(ConsistencyLevel consistency) {
            buffer.append(String.format(symbols.consistency, consistency == null ? symbols.unsetValue : consistency));
            return this;
        }

        public StatementWriter appendSerialConsistency(ConsistencyLevel serialConsistency) {
            buffer.append(String.format(symbols.serialConsistency, serialConsistency == null ? symbols.unsetValue : serialConsistency));
            return this;
        }

        public StatementWriter appendDefaultTimestamp(long defaultTimestamp) {
            buffer.append(String.format(symbols.defaultTimestamp, defaultTimestamp == Long.MIN_VALUE ? symbols.unsetValue : defaultTimestamp));
            return this;
        }

        public StatementWriter appendReadTimeoutMillis(int readTimeoutMillis) {
            buffer.append(String.format(symbols.readTimeoutMillis, readTimeoutMillis == Integer.MIN_VALUE ? symbols.unsetValue : readTimeoutMillis));
            return this;
        }

        public StatementWriter appendIdempotent(Boolean idempotent) {
            buffer.append(String.format(symbols.idempotent, idempotent == null ? symbols.unsetValue : idempotent));
            return this;
        }

        public StatementWriter appendBatchType(BatchStatement.Type batchType) {
            buffer.append(batchType);
            return this;
        }

        /**
         * Appends the given fragment as a query string fragment.
         * <p>
         * This method can be called multiple times, in case the printer
         * needs to compute the query string by pieces.
         * <p>
         * This methods also keeps track of the amount of characters used so far
         * to print the query string, and automatically detects when
         * the query string exceeds {@link StatementFormatterLimits#maxQueryStringLength the maximum length},
         * in which case it appends only once the {@link StatementFormatterSymbols#truncatedOutput truncated output} symbol.
         *
         * @param queryStringFragment The query string fragment to append
         * @return this writer (for method chaining).
         */
        public StatementWriter appendQueryStringFragment(String queryStringFragment) {
            if (maxQueryStringLengthExceeded())
                return this;
            else if (queryStringFragment.isEmpty())
                return this;
            else if (limits.maxQueryStringLength == StatementFormatterLimits.UNLIMITED)
                buffer.append(queryStringFragment);
            else if (queryStringFragment.length() > remainingQueryStringChars) {
                if (remainingQueryStringChars > 0) {
                    queryStringFragment = queryStringFragment.substring(0, remainingQueryStringChars);
                    buffer.append(queryStringFragment);
                }
                appendTruncatedOutput();
                remainingQueryStringChars = MAX_EXCEEDED;
            } else {
                buffer.append(queryStringFragment);
                remainingQueryStringChars -= queryStringFragment.length();
            }
            return this;
        }

        /**
         * Appends the statement's outgoing payload.
         *
         * @param outgoingPayload The payload to append
         * @return this
         */
        public StatementWriter appendOutgoingPayload(Map<String, ByteBuffer> outgoingPayload) {
            int remaining = limits.maxOutgoingPayloadEntries;
            Iterator<Map.Entry<String, ByteBuffer>> it = outgoingPayload.entrySet().iterator();
            if (it.hasNext()) {
                appendOutgoingPayloadStart();
                while (it.hasNext()) {
                    if (limits.maxOutgoingPayloadEntries != StatementFormatterLimits.UNLIMITED && remaining == 0) {
                        appendTruncatedOutput();
                        break;
                    }
                    Map.Entry<String, ByteBuffer> entry = it.next();
                    String name = entry.getKey();
                    buffer.append(name);
                    appendNameValueSeparator();
                    ByteBuffer value = entry.getValue();
                    String formatted;
                    boolean lengthExceeded = false;
                    if (value == null) {
                        formatted = symbols.nullValue;
                    } else {
                        if (limits.maxOutgoingPayloadValueLength != StatementFormatterLimits.UNLIMITED) {
                            // prevent large blobs from being converted to strings
                            lengthExceeded = value.remaining() > limits.maxOutgoingPayloadValueLength;
                            if (lengthExceeded)
                                value = (ByteBuffer) value.duplicate().limit(limits.maxOutgoingPayloadValueLength);
                        }
                        formatted = TypeCodec.blob().format(value);
                    }
                    buffer.append(formatted);
                    if (lengthExceeded)
                        appendTruncatedOutput();
                    if (it.hasNext())
                        appendListElementSeparator();
                    if (limits.maxOutgoingPayloadEntries != StatementFormatterLimits.UNLIMITED)
                        remaining--;
                }
                appendOutgoingPayloadEnd();
            }
            return this;
        }

        public StatementWriter appendBoundValue(int index, ByteBuffer serialized, DataType type) {
            if (maxAppendedBoundValuesExceeded())
                return this;
            return appendBoundValue(Integer.toString(index), serialized, type);
        }

        public StatementWriter appendBoundValue(String name, ByteBuffer serialized, DataType type) {
            if (maxAppendedBoundValuesExceeded())
                return this;
            TypeCodec<Object> codec = codecRegistry.codecFor(type);
            Object value = codec.deserialize(serialized, protocolVersion);
            return appendBoundValue(name, value, type);
        }

        public StatementWriter appendBoundValue(int index, Object value, DataType type) {
            if (maxAppendedBoundValuesExceeded())
                return this;
            return appendBoundValue(Integer.toString(index), value, type);
        }

        public StatementWriter appendBoundValue(String name, Object value, DataType type) {
            if (maxAppendedBoundValuesExceeded())
                return this;
            if (value == null) {
                doAppendBoundValue(name, symbols.nullValue);
                return this;
            } else if (value instanceof ByteBuffer && limits.maxBoundValueLength != StatementFormatterLimits.UNLIMITED) {
                ByteBuffer byteBuffer = (ByteBuffer) value;
                int maxBufferLengthInBytes = Math.max(2, limits.maxBoundValueLength / 2) - 1;
                boolean bufferLengthExceeded = byteBuffer.remaining() > maxBufferLengthInBytes;
                // prevent large blobs from being converted to strings
                if (bufferLengthExceeded) {
                    byteBuffer = (ByteBuffer) byteBuffer.duplicate().limit(maxBufferLengthInBytes);
                    // force usage of blob codec as any other codec would probably fail to format
                    // a cropped byte buffer anyway
                    String formatted = TypeCodec.blob().format(byteBuffer);
                    doAppendBoundValue(name, formatted);
                    appendTruncatedOutput();
                    return this;
                }
            }
            TypeCodec<Object> codec = type == null ? codecRegistry.codecFor(value) : codecRegistry.codecFor(type, value);
            doAppendBoundValue(name, codec.format(value));
            return this;
        }

        public StatementWriter appendUnsetBoundValue(int index) {
            return appendUnsetBoundValue(Integer.toString(index));
        }

        public StatementWriter appendUnsetBoundValue(String name) {
            doAppendBoundValue(name, symbols.unsetValue);
            return this;
        }

        private void doAppendBoundValue(String name, String value) {
            if (maxAppendedBoundValuesExceeded())
                return;
            if (remainingBoundValues == 0) {
                appendTruncatedOutput();
                remainingBoundValues = MAX_EXCEEDED;
                return;
            }
            boolean lengthExceeded = false;
            if (limits.maxBoundValueLength != StatementFormatterLimits.UNLIMITED && value.length() > limits.maxBoundValueLength) {
                value = value.substring(0, limits.maxBoundValueLength);
                lengthExceeded = true;
            }
            if (name != null) {
                buffer.append(name);
                appendNameValueSeparator();
            }
            buffer.append(value);
            if (lengthExceeded)
                appendTruncatedOutput();
            if (limits.maxBoundValues != StatementFormatterLimits.UNLIMITED)
                remainingBoundValues--;
        }

        @Override
        public String toString() {
            return buffer.toString();
        }

    }

    /**
     * A common parent class for {@link StatementPrinter} implementations.
     * <p/>
     * This class assumes a common formatting pattern comprised of the following
     * sections:
     * <ol>
     * <li>Header: this section should contain two subsections:
     * <ol>
     *     <li>The actual statement class and the statement's hash code;</li>
     *     <li>The statement "summary"; examples of typical information
     *     that could be included here are: the statement's consistency level;
     *     its default timestamp; its idempotence flag; the number of bound values; etc.</li>
     * </ol>
     * </li>
     * <li>Query String: this section should print the statement's query string, if it is available;
     * this section is only enabled if the verbosity is {@link StatementFormatVerbosity#NORMAL NORMAL} or higher;</li>
     * <li>Bound Values: this section should print the statement's bound values, if available;
     * this section is only enabled if the verbosity is {@link StatementFormatVerbosity#EXTENDED EXTENDED};</li>
     * <li>Outgoing Payload: this section should print the statement's outgoing payload, if available;
     * this section is only enabled if the verbosity is {@link StatementFormatVerbosity#EXTENDED EXTENDED};</li>
     * <li>Footer: an optional section, empty by default.</li>
     * </ol>
     */
    public static class StatementPrinterBase<S extends Statement> implements StatementPrinter<S> {

        private final Class<S> supportedStatementClass;

        protected StatementPrinterBase(Class<S> supportedStatementClass) {
            this.supportedStatementClass = supportedStatementClass;
        }

        @Override
        public Class<S> getSupportedStatementClass() {
            return supportedStatementClass;
        }

        @Override
        public void print(S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            printHeader(statement, out, verbosity);
            if (verbosity.compareTo(StatementFormatVerbosity.NORMAL) >= 0) {
                printQueryString(statement, out, verbosity);
                if (verbosity.compareTo(StatementFormatVerbosity.EXTENDED) >= 0) {
                    printBoundValues(statement, out, verbosity);
                    printOutgoingPayload(statement, out, verbosity);
                }
            }
            printFooter(statement, out, verbosity);
        }

        protected void printHeader(S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            out.appendClassNameAndHashCode(statement);
            out.appendSummaryStart();
            printSummary(statement, out, verbosity);
            out.appendSummaryEnd();
        }

        protected void printSummary(S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            out.appendIdempotent(statement.isIdempotent());
            out.appendListElementSeparator();
            out.appendConsistency(statement.getConsistencyLevel());
            if (verbosity.compareTo(StatementFormatVerbosity.NORMAL) >= 0) {
                out.appendListElementSeparator();
                out.appendSerialConsistency(statement.getSerialConsistencyLevel());
                out.appendListElementSeparator();
                out.appendDefaultTimestamp(statement.getDefaultTimestamp());
                out.appendListElementSeparator();
                out.appendReadTimeoutMillis(statement.getReadTimeoutMillis());
            }
        }

        protected void printQueryString(S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
        }

        protected void printBoundValues(S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
        }

        protected void printOutgoingPayload(S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            Map<String, ByteBuffer> outgoingPayload = statement.getOutgoingPayload();
            if (outgoingPayload != null)
                out.appendOutgoingPayload(outgoingPayload);
        }

        protected void printFooter(S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
        }

    }

    /**
     * Common parent class for {@link StatementPrinter} implementations dealing with subclasses of {@link RegularStatement}.
     */
    public abstract static class AbstractRegularStatementPrinter<S extends RegularStatement> extends StatementPrinterBase<S> {

        protected AbstractRegularStatementPrinter(Class<S> supportedStatementClass) {
            super(supportedStatementClass);
        }

        @Override
        protected void printQueryString(S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            out.appendQueryStringStart();
            out.appendQueryStringFragment(statement.getQueryString(out.getCodecRegistry()));
            out.appendQueryStringEnd();
        }

    }

    public static class SimpleStatementPrinter extends AbstractRegularStatementPrinter<SimpleStatement> {

        public SimpleStatementPrinter() {
            super(SimpleStatement.class);
        }

        @Override
        protected void printSummary(SimpleStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            super.printSummary(statement, out, verbosity);
            out.appendListElementSeparator();
            out.appendBoundValuesCount(statement.valuesCount());
        }

        @Override
        protected void printBoundValues(SimpleStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            if (statement.valuesCount() > 0) {
                out.appendBoundValuesStart();
                if (statement.usesNamedValues()) {
                    boolean first = true;
                    for (String valueName : statement.getValueNames()) {
                        if (first)
                            first = false;
                        else
                            out.appendListElementSeparator();
                        out.appendBoundValue(valueName, statement.getObject(valueName), null);
                        if (out.maxAppendedBoundValuesExceeded())
                            break;
                    }
                } else {
                    for (int i = 0; i < statement.valuesCount(); i++) {
                        if (i > 0)
                            out.appendListElementSeparator();
                        out.appendBoundValue(i, statement.getObject(i), null);
                        if (out.maxAppendedBoundValuesExceeded())
                            break;
                    }
                }
                out.appendBoundValuesEnd();
            }
        }

    }

    public static class BuiltStatementPrinter extends AbstractRegularStatementPrinter<BuiltStatement> {

        public BuiltStatementPrinter() {
            super(BuiltStatement.class);
        }

        @Override
        protected void printSummary(BuiltStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            super.printSummary(statement, out, verbosity);
            out.appendListElementSeparator();
            out.appendBoundValuesCount(statement.valuesCount(out.getCodecRegistry()));
        }

        @Override
        protected void printBoundValues(BuiltStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            if (statement.valuesCount(out.getCodecRegistry()) > 0) {
                out.appendBoundValuesStart();
                // BuiltStatement does not use named values
                for (int i = 0; i < statement.valuesCount(out.getCodecRegistry()); i++) {
                    if (i > 0)
                        out.appendListElementSeparator();
                    out.appendBoundValue(i, statement.getObject(i), null);
                    if (out.maxAppendedBoundValuesExceeded())
                        break;
                }
                out.appendBoundValuesEnd();
            }
        }

    }

    public static class BoundStatementPrinter extends StatementPrinterBase<BoundStatement> {

        public BoundStatementPrinter() {
            super(BoundStatement.class);
        }

        @Override
        protected void printSummary(BoundStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            super.printSummary(statement, out, verbosity);
            out.appendListElementSeparator();
            ColumnDefinitions metadata = statement.preparedStatement().getVariables();
            out.appendBoundValuesCount(metadata.size());
        }

        @Override
        protected void printQueryString(BoundStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            out.appendQueryStringStart();
                out.appendQueryStringFragment(statement.preparedStatement().getQueryString());
            out.appendQueryStringEnd();
        }

        @Override
        protected void printBoundValues(BoundStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            if (statement.preparedStatement().getVariables().size() > 0) {
                out.appendBoundValuesStart();
                ColumnDefinitions metadata = statement.preparedStatement().getVariables();
                if (metadata.size() > 0) {
                    for (int i = 0; i < metadata.size(); i++) {
                        if (i > 0)
                            out.appendListElementSeparator();
                        if (statement.isSet(i))
                            out.appendBoundValue(metadata.getName(i), statement.wrapper.values[i], metadata.getType(i));
                        else
                            out.appendUnsetBoundValue(metadata.getName(i));
                        if (out.maxAppendedBoundValuesExceeded())
                            break;
                    }
                }
                out.appendBoundValuesEnd();
            }
        }

    }

    public static class BatchStatementPrinter implements StatementPrinter<BatchStatement> {

        @Override
        public Class<BatchStatement> getSupportedStatementClass() {
            return BatchStatement.class;
        }

        @Override
        public void print(BatchStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            out.appendClassNameAndHashCode(statement);
            out.appendSummaryStart();
            out.appendBatchType(getBatchType(statement));
            out.appendListElementSeparator();
            out.appendInnerStatementsCount(statement.size());
            out.appendListElementSeparator();
            int totalBoundValuesCount = 0;
            for (List<ByteBuffer> values : getValues(statement, out)) {
                totalBoundValuesCount += values.size();
            }
            out.appendBoundValuesCount(totalBoundValuesCount);
            out.appendSummaryEnd();
            if (verbosity.compareTo(StatementFormatVerbosity.NORMAL) >= 0 && out.getLimits().maxInnerStatements > 0) {
                out.getBuffer().append(' ');
                int i = 1;
                for (Statement stmt : statement.getStatements()) {
                    if (i > 1)
                        out.appendListElementSeparator();
                    if (i > out.getLimits().maxInnerStatements) {
                        out.appendTruncatedOutput();
                        break;
                    }
                    out.getBuffer().append(i++);
                    out.appendNameValueSeparator();
                    StatementPrinter<? super Statement> printer = out.getPrinterRegistry().findPrinter(stmt);
                    printer.print(stmt, out.createChildWriter(), verbosity);
                }
            }
        }

        protected BatchStatement.Type getBatchType(BatchStatement statement) {
            return statement.batchType;
        }

        protected List<List<ByteBuffer>> getValues(BatchStatement statement, StatementWriter out) {
            return statement.getIdAndValues(out.getProtocolVersion(), out.getCodecRegistry()).values;
        }

    }

    public static class StatementWrapperPrinter implements StatementPrinter<StatementWrapper> {

        @Override
        public Class<StatementWrapper> getSupportedStatementClass() {
            return StatementWrapper.class;
        }

        @Override
        public void print(StatementWrapper statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            Statement wrappedStatement = statement.getWrappedStatement();
            StatementPrinter<? super Statement> printer = out.getPrinterRegistry().findPrinter(wrappedStatement);
            printer.print(wrappedStatement, out, verbosity);
        }

    }

    public static class SchemaStatementPrinter extends AbstractRegularStatementPrinter<SchemaStatement> {

        public SchemaStatementPrinter() {
            super(SchemaStatement.class);
        }

    }

    public static class RegularStatementPrinter extends AbstractRegularStatementPrinter<RegularStatement> {

        public RegularStatementPrinter() {
            super(RegularStatement.class);
        }

    }

    public static class DefaultStatementPrinter extends StatementPrinterBase<Statement> {

        public DefaultStatementPrinter() {
            super(Statement.class);
        }

    }

    /**
     * Helper class to build {@link StatementFormatter} instances with a fluent API.
     */
    public static class Builder {

        private final List<StatementPrinter> printers = new ArrayList<StatementPrinter>();
        private StatementFormatterLimits limits = new StatementFormatterLimits();
        private StatementFormatterSymbols symbols = new StatementFormatterSymbols();

        private Builder() {
        }

        public Builder withLimits(StatementFormatterLimits limits) {
            this.limits = limits;
            return this;
        }

        public Builder withSymbols(StatementFormatterSymbols symbols) {
            this.symbols = symbols;
            return this;
        }

        public Builder addStatementPrinter(StatementPrinter<?> printer) {
            printers.add(printer);
            return this;
        }

        public Builder addStatementPrinters(StatementPrinter<?>... printers) {
            this.printers.addAll(Arrays.asList(printers));
            return this;
        }

        /**
         * Build the {@link StatementFormatter} instance.
         *
         * @return the {@link StatementFormatter} instance.
         * @throws IllegalArgumentException if the builder is unable to build a valid instance due to incorrect settings.
         */
        public StatementFormatter build() {
            StatementPrinterRegistry registry = new StatementPrinterRegistry();
            registerDefaultPrinters(registry);
            for (StatementPrinter printer : printers) {
                registry.register(printer);
            }
            return new StatementFormatter(registry, limits, symbols);
        }

        private void registerDefaultPrinters(StatementPrinterRegistry registry) {
            registry.register(new DefaultStatementPrinter());
            registry.register(new SimpleStatementPrinter());
            registry.register(new RegularStatementPrinter());
            registry.register(new BuiltStatementPrinter());
            registry.register(new SchemaStatementPrinter());
            registry.register(new BoundStatementPrinter());
            registry.register(new BatchStatementPrinter());
            registry.register(new StatementWrapperPrinter());
        }

    }

    private final StatementPrinterRegistry printerRegistry;
    private final StatementFormatterLimits limits;
    private final StatementFormatterSymbols symbols;

    private StatementFormatter(StatementPrinterRegistry printerRegistry, StatementFormatterLimits limits, StatementFormatterSymbols symbols) {
        this.printerRegistry = printerRegistry;
        this.limits = limits;
        this.symbols = symbols;
    }

    /**
     * Formats the given {@link Statement statement}.
     *
     * @param statement       The statement to format.
     * @param verbosity       The verbosity to use.
     * @param protocolVersion The protocol version in use.
     * @param codecRegistry   The codec registry in use.
     * @return The statement as a formatted string.
     * @throws StatementFormatException if the formatting failed.
     */
    public <S extends Statement> String format(
            S statement, StatementFormatVerbosity verbosity,
            ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        try {
            StatementPrinter<? super S> printer = printerRegistry.findPrinter(statement);
            assert printer != null : "Could not find printer for statement class " + statement.getClass();
            StatementWriter out = new StatementWriter(new StringBuilder(), printerRegistry, limits, symbols, protocolVersion, codecRegistry);
            printer.print(statement, out, verbosity);
            return out.toString().trim();
        } catch (RuntimeException e) {
            throw new StatementFormatException(statement, verbosity, protocolVersion, codecRegistry, e);
        }
    }

}
