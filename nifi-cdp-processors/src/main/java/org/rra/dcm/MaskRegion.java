package org.rra.dcm;

import lombok.Data;
import lombok.ToString;
import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class MaskRegion {

    private int x;
    private int y;
    private int width;
    private int height;

    public MaskRegion(
            int x,
            int y,
            int width,
            int height
    ) {
        if (x < 0 || y < 0) {
            throw new IllegalArgumentException(
                    "x und y dürfen nicht negativ sein"
            );
        }

        if (width == 0) {
            throw new IllegalArgumentException(
                    "width darf nicht 0 sein. Eine negative Breite "
                            + "verwendet die gesamte Bildbreite."
            );
        }

        if (height <= 0) {
            throw new IllegalArgumentException(
                    "height muss größer als 0 sein"
            );
        }

        this.x = x;
        this.y = y;
        this.width = width;
        this.height = height;
    }
}