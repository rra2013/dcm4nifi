# dcm4nifi – DICOM & HL7 Toolkit for Apache NiFi

![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-2.4-blue)
![DICOM](https://img.shields.io/badge/DICOM-supported-green)
![HL7](https://img.shields.io/badge/HL7-v2-orange)
![License](https://img.shields.io/badge/license-MIT-blue)
![dcm4che](https://img.shields.io/badge/dcm4che-DICOM%20Toolkit-blueviolet)

dcm4nifi is an open-source DICOM and HL7 integration toolkit for Apache NiFi, enabling seamless healthcare data processing, pseudonymization, and anonymization for AI-ready medical data pipelines.

dcm4nifi brings native DICOM capabilities to Apache NiFi, allowing the implementation of scalable and secure DICOM workflows within NiFi-based data pipelines.

> A powerful extension for Apache NiFi to process, transform, and securely prepare DICOM and HL7 data for research, interoperability, and machine learning applications.

---

## Introduction

Unlock seamless healthcare data integration with a powerful DICOM and HL7 toolset for Apache NiFi 2.4. Built on the robust HAPI and dcm4che libraries, this solution enables efficient processing of medical data while ensuring secure pseudonymization and anonymization—making it ideal for modern, AI-driven research and scalable digital health initiatives.

---

## Advantages of Using Apache NiFi Compared to Proprietary Solutions

Apache NiFi, as an open-source data integration platform, offers a cost-effective, flexible, and extensible alternative to proprietary systems for automating data flows and real-time integrations. Compared to commercial solutions, NiFi eliminates licensing costs entirely, while its visual development environment and modular architecture significantly reduce development time and maintenance effort.

Thanks to open standards and the ability to easily integrate custom processors (e.g., in Java), NiFi is ideally suited for heterogeneous system landscapes and customer-specific requirements. Companies benefit from significant cost savings, greater adaptability, and a reduced time-to-market. This makes NiFi particularly well-suited for dynamic data environments and digital transformation projects.

---

## DICOM Processing with Apache NiFi

dcm4nifi enables native DICOM processing within Apache NiFi, allowing users to build scalable and flexible data pipelines for medical imaging workflows.

With dcm4nifi, Apache NiFi can be used to:

- Process and route DICOM data within NiFi pipelines
- Integrate PACS, RIS, and other radiology systems
- Perform DICOM anonymization and pseudonymization
- Prepare medical imaging data for AI and machine learning workflows

This makes Apache NiFi a powerful platform for handling DICOM-based workflows in modern healthcare and research environments.

---

## DICOM & HL7 Toolset

I developed a specialized DICOM and HL7 toolset for Apache NiFi 2.4, built on the proven HAPI and dcm4che libraries, to enable seamless integration of healthcare data standards into modern data flow architectures. The toolkit extends Apache NiFi with processors and components specifically designed for handling, transforming, routing, and validating medical messaging and imaging data.

By combining HL7 messaging capabilities with DICOM-based imaging workflows, the toolset supports the integration of clinical and radiological systems such as HIS, RIS, PACS, and other healthcare applications. It enables the processing of HL7 v2 messages for admissions, orders, results, and other clinical events, while also supporting DICOM communication and metadata handling for imaging-related workflows.

Built on HAPI, the toolkit provides robust support for parsing, validating, and generating HL7 messages in a standards-compliant manner. Through dcm4che, it leverages a mature and widely adopted framework for DICOM networking, file handling, and metadata operations. This foundation ensures reliability, interoperability, and compatibility with existing healthcare IT environments.

A key feature of the toolset is its strong support for **pseudonymization and anonymization of DICOM data**, which is particularly critical in research and data-driven healthcare initiatives. The toolkit enables flexible de-identification workflows, allowing sensitive patient information to be removed or replaced in compliance with data protection regulations while preserving the usability of imaging data.

This capability is especially important in today’s landscape, where large volumes of medical imaging data are required to train AI and machine learning models. By facilitating secure and compliant data preparation, the toolset helps organizations unlock the value of clinical data for research and innovation without compromising patient privacy.

Integrated into Apache NiFi’s visual flow-based environment, the toolset allows users to design and operate complex healthcare integration scenarios with minimal coding effort. At the same time, it remains highly extensible, making it suitable for custom use cases, institution-specific workflows, and evolving interoperability requirements.

The result is a flexible and scalable solution for real-time healthcare data exchange, capable of reducing implementation effort, improving interoperability between medical systems, and accelerating the delivery of digital healthcare and AI-driven research projects.

---

## Security & Data Handling

Handling sensitive healthcare data requires strict security and compliance measures. This toolkit is designed with privacy and data protection in mind, supporting secure and compliant data processing workflows.

### Pseudonymization and Anonymization

The toolkit provides flexible mechanisms for pseudonymization and anonymization of DICOM data. Sensitive patient information can be removed or replaced while preserving the integrity and usability of medical imaging data for research and AI applications.

### Secure Data Processing

Built on Apache NiFi, the toolset benefits from secure data flow management, including:

- Fine-grained access control
- Data provenance and audit tracking
- Secure data routing and transformation

### Compliance

The toolkit supports data handling practices aligned with regulations such as GDPR (EU) and DSG (Switzerland), enabling organizations to safely use medical data for research and machine learning without compromising patient privacy.

---

## Key Features

- HL7 v2 message processing (parsing, validation, transformation)
- DICOM networking and file handling (C-STORE SCP/SCU, C-FIND, C-MOVE, C-GET)
- DICOM compression and decompression for optimized storage and efficient data transfer
- Flexible DICOM tag manipulation and metadata transformation
- Seamless integration with HIS, RIS, and PACS
- Visual flow-based orchestration with Apache NiFi
- **DICOM pseudonymization and anonymization for research**
- AI-ready data preparation pipelines
- Extensible architecture for custom processors

---

## Why It Matters

This toolkit enables healthcare organizations to securely unlock the value of medical data for AI, research, and digital transformation—without compromising patient privacy.

---

## Important

Please use the `master` branch for the latest stable version.

---

## License

This project is licensed under the MIT License.

See [LICENSE](./LICENSE) for details.

---

## Third-Party Licenses

This project depends on third-party libraries, including:

- dcm4che (Apache License 2.0)
- HAPI HL7 (Apache License 2.0)

For full license information, see [NOTICE](./NOTICE).