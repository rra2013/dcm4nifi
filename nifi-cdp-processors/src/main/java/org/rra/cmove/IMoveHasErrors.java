package org.rra.cmove;

@FunctionalInterface
public interface IMoveHasErrors {
    void moveHasError(int status, String message) throws Exception;
}
