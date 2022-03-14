package com.github.cpzt.connector.url.reader;

import java.io.Serializable;
import java.util.function.Function;

public interface IFunction<T, R> extends Function<T, R>, Serializable {

}
